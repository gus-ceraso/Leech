package utp

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

// ErrNotConnected is returned by the stream methods while an outgoing uTP
// handshake is still in progress. DialContext does not return the Conn until
// the handshake has completed, but NewConn is useful to transport tests.
var ErrNotConnected = errors.New("utp: connection is not established")

// Conn is one outgoing uTP connection. The UDP socket and both state machines
// are owned by one worker; application methods only hold mu while inspecting
// or changing the bounded state. There is deliberately no listener or server
// side in this package.
type Conn struct {
	mu sync.Mutex

	udp    *net.UDPConn
	local  net.Addr
	remote net.Addr

	send *SendState
	recv *ReceiveState

	// recvID is the connection ID on packets received from the peer. sendID
	// is the ID on packets sent after SYN. The initiating SYN itself uses
	// recvID, as required by BEP 29.
	recvID uint16
	sendID uint16
	synSeq Sequence

	established bool
	remoteFIN   bool
	terminal    error
	closed      bool

	readDeadline  time.Time
	writeDeadline time.Time

	readWake  chan struct{}
	writeWake chan struct{}
	workWake  chan struct{}
	estWake   chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

const workerPoll = 25 * time.Millisecond

// NewConn starts an outgoing uTP handshake on an already connected UDP
// socket. The caller retains no socket ownership after success: Close on the
// returned Conn closes it. The socket must have a remote address, as returned
// by net.Dialer.DialContext or net.DialUDP.
func NewConn(ctx context.Context, udp *net.UDPConn) (*Conn, error) {
	if udp == nil {
		return nil, errors.New("utp: nil UDP socket")
	}
	if udp.RemoteAddr() == nil {
		return nil, errors.New("utp: UDP socket is not connected")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	connID, err := randomUint16()
	if err != nil {
		return nil, err
	}
	// BEP 29 initializes an outgoing SYN sequence number to one. The peer's
	// sequence is random and is learned from its handshake STATE packet.
	send, err := NewSendStateWithConfig(Sequence(1), SendConfig{ConnectionID: connID})
	if err != nil {
		return nil, err
	}
	now := time.Now()
	actions, err := send.Start(now)
	if err != nil {
		return nil, err
	}
	c := &Conn{
		udp:       udp,
		local:     udp.LocalAddr(),
		remote:    udp.RemoteAddr(),
		send:      send,
		recvID:    connID,
		sendID:    connID + 1,
		synSeq:    actions[0].Packet.SeqNr,
		readWake:  make(chan struct{}),
		writeWake: make(chan struct{}),
		workWake:  make(chan struct{}, 1),
		estWake:   make(chan struct{}),
		done:      make(chan struct{}),
	}
	// SYN is the only packet sent with recvID. Retransmissions retain that
	// immutable packet in SendState; all subsequent actions use sendID.
	send.SetConnectionID(c.sendID)
	go c.run(ctx, actions)
	return c, nil
}

func randomUint16() (uint16, error) {
	var value [2]byte
	if _, err := rand.Read(value[:]); err != nil {
		return 0, err
	}
	return uint16(value[0])<<8 | uint16(value[1]), nil
}

// Read implements net.Conn. Ordered bytes retained by ReceiveState are
// returned before EOF, RESET, cancellation, or a local close is reported.
func (c *Conn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		c.mu.Lock()
		if c.recv != nil {
			windowBefore := c.recv.WindowSize()
			n, err := c.recv.Read(p)
			if n != 0 || err != nil {
				if n != 0 && c.established && !c.closed {
					now := time.Now()
					c.syncSendStateLocked(now, nil)
					if windowBefore == 0 && c.recv.WindowSize() > windowBefore {
						if ackErr := c.writeActionLocked(c.receiveACKLocked(now)); ackErr != nil {
							c.setTerminalLocked(ackErr)
						}
					}
				}
				c.mu.Unlock()
				return n, err
			}
		}
		if c.terminal != nil && (c.recv == nil || c.closed) {
			err := c.terminal
			c.mu.Unlock()
			return 0, err
		}
		wake := c.readWake
		deadline := c.readDeadline
		done := c.done
		c.mu.Unlock()

		if err := waitForWake(wake, done, deadline); err != nil {
			return 0, err
		}
	}
}

// Write queues p in the bounded SendState and waits when the send queue and
// unacknowledged packet budget are full. Queue copies each accepted prefix,
// so the caller may reuse p after Write returns.
func (c *Conn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	written := 0
	for written < len(p) {
		c.mu.Lock()
		if c.closed || c.terminal != nil {
			err := c.writeErrorLocked()
			c.mu.Unlock()
			return written, err
		}
		if !c.established {
			c.mu.Unlock()
			return written, ErrNotConnected
		}
		if c.remoteFIN {
			c.mu.Unlock()
			return written, io.ErrClosedPipe
		}
		accepted, queueErr := c.send.Queue(p[written:])
		written += accepted
		if accepted != 0 {
			c.wakeWorker()
		}
		wake := c.writeWake
		deadline := c.writeDeadline
		done := c.done
		c.mu.Unlock()

		if written == len(p) {
			return written, nil
		}
		if queueErr != nil && !errors.Is(queueErr, ErrSendQueueFull) {
			return written, queueErr
		}
		if err := waitForWake(wake, done, deadline); err != nil {
			return written, err
		}
	}
	return written, nil
}

func (c *Conn) writeErrorLocked() error {
	if c.terminal != nil {
		return c.terminal
	}
	if c.closed {
		return net.ErrClosed
	}
	return io.ErrClosedPipe
}

func waitForWake(wake <-chan struct{}, done <-chan struct{}, deadline time.Time) error {
	if !deadline.IsZero() {
		if !time.Now().Before(deadline) {
			return os.ErrDeadlineExceeded
		}
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		select {
		case <-wake:
			return nil
		case <-done:
			// Recheck the state in the caller. This preserves a peer RESET or
			// transport error instead of racing the worker's final close.
			return nil
		case <-timer.C:
			return os.ErrDeadlineExceeded
		}
	}
	select {
	case <-wake:
		return nil
	case <-done:
		return nil
	}
}

func (c *Conn) LocalAddr() net.Addr  { return c.local }
func (c *Conn) RemoteAddr() net.Addr { return c.remote }

func (c *Conn) SetDeadline(deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	c.readDeadline = deadline
	c.writeDeadline = deadline
	signal(&c.readWake)
	signal(&c.writeWake)
	return nil
}

func (c *Conn) SetReadDeadline(deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	c.readDeadline = deadline
	signal(&c.readWake)
	return nil
}

func (c *Conn) SetWriteDeadline(deadline time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	c.writeDeadline = deadline
	signal(&c.writeWake)
	return nil
}

// Close is idempotent and joins the socket worker. A connected peer receives a
// best-effort RESET before the UDP socket is closed; pending application I/O is
// woken immediately and never depends on a remote response.
func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		if c.established {
			c.writeResetLocked()
		}
		c.terminal = net.ErrClosed
		signal(&c.readWake)
		signal(&c.writeWake)
		_ = c.udp.Close()
		c.mu.Unlock()
	})
	<-c.done
	return nil
}

func signal(ch *chan struct{}) {
	select {
	case <-*ch:
		*ch = make(chan struct{})
	default:
		close(*ch)
		*ch = make(chan struct{})
	}
}

func (c *Conn) wakeWorker() {
	select {
	case c.workWake <- struct{}{}:
	default:
	}
	// Interrupt a pending ReadFromUDP so queued bytes are packetized promptly.
	_ = c.udp.SetReadDeadline(time.Now())
}

func (c *Conn) run(ctx context.Context, initial []PacketAction) {
	defer func() {
		_ = c.udp.Close()
		close(c.done)
	}()
	for _, action := range initial {
		if action.Kind == ActionSend {
			if err := c.writeAction(action.Packet); err != nil {
				c.fail(err)
				return
			}
		}
	}
	buf := make([]byte, 64<<10)
	for {
		if ctx != nil {
			select {
			case <-ctx.Done():
				c.mu.Lock()
				established := c.established
				c.mu.Unlock()
				if !established {
					c.fail(ctx.Err())
					return
				}
			default:
			}
		}
		_ = c.udp.SetReadDeadline(time.Now().Add(workerPoll))
		n, err := c.udp.Read(buf)
		if err == nil {
			c.handleDatagram(buf[:n])
		} else if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
			c.mu.Lock()
			closed := c.closed
			c.mu.Unlock()
			if !closed {
				c.fail(err)
				return
			}
		}
		c.service()
		c.mu.Lock()
		terminal := c.closed || c.terminal != nil
		c.mu.Unlock()
		if terminal {
			return
		}
	}
}

func (c *Conn) service() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.send == nil {
		return
	}
	now := time.Now()
	actions := c.send.Tick(now)
	if c.established {
		actions = append(actions, c.send.Produce(now)...)
	}
	for _, action := range actions {
		if action.Kind == ActionSend {
			if err := c.writeActionLocked(action.Packet); err != nil {
				c.setTerminalLocked(err)
				return
			}
		}
	}
	if c.send.Finished() {
		signal(&c.writeWake)
	}
}

func (c *Conn) handleDatagram(wire []byte) {
	packet, err := ParsePacket(wire)
	if err != nil {
		// A connected UDP socket can still receive a malformed datagram from
		// the peer. It carries no state that can safely be applied.
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.terminal != nil {
		return
	}
	if packet.ConnectionID != c.recvID {
		return
	}
	now := time.Now()
	if !c.established {
		if packet.Type != State {
			if packet.Type == Reset {
				c.setTerminalLocked(ErrReceiveReset)
			}
			return
		}
		if packet.AckNr != c.synSeq {
			// A STATE with the right connection ID but an unrelated ACK is
			// not our handshake. Leave SYN retransmission and the caller's
			// context in charge of eventual failure.
			return
		}
		// The peer's STATE both acknowledges our SYN and supplies its first
		// sequence number. Do not accept data until this transition succeeds.
		c.recv = NewReceiveState(packet.SeqNr.Add(1))
		c.syncSendStateLocked(now, &packet)
		result := c.send.Handle(packet, now)
		if result.Err != nil {
			c.setTerminalLocked(result.Err)
			return
		}
		c.established = true
		signal(&c.estWake)
		for _, action := range result.Actions {
			if action.Kind == ActionSend {
				if err := c.writeActionLocked(action.Packet); err != nil {
					c.setTerminalLocked(err)
					return
				}
			}
		}
		signal(&c.readWake)
		signal(&c.writeWake)
		return
	}

	received := c.recv.Receive(packet)
	if c.recv.Err() != nil {
		c.setTerminalLocked(c.recv.Err())
		return
	}
	c.syncSendStateLocked(now, &packet)
	result := c.send.Handle(packet, now)
	if result.Err != nil {
		c.setTerminalLocked(result.Err)
		return
	}
	if result.AckedBytes > 0 {
		// Write may be blocked on the bounded queue. An ACK returns exactly
		// that many bytes of queue credit, so wake the waiting application
		// writer after applying the ACK state transition.
		signal(&c.writeWake)
	}
	for _, action := range result.Actions {
		if action.Kind == ActionSend {
			if err := c.writeActionLocked(action.Packet); err != nil {
				c.setTerminalLocked(err)
				return
			}
		}
	}
	for _, action := range received.Actions {
		if action.Kind == ActionSend {
			ack := c.stampReceiveACKLocked(action.Packet, now)
			if err := c.writeActionLocked(ack); err != nil {
				c.setTerminalLocked(err)
				return
			}
		}
	}
	if c.recv.Finished() {
		c.remoteFIN = true
		signal(&c.writeWake)
	}
	signal(&c.readWake)
	if c.recv.Err() != nil {
		c.setTerminalLocked(c.recv.Err())
	}
}

func (c *Conn) writeAction(packet Packet) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeActionLocked(packet)
}

func (c *Conn) writeActionLocked(packet Packet) error {
	if c.established && c.recv != nil && c.send != nil && packet.Type != Syn {
		// Retransmissions retain their payload and sequence number in SendState,
		// but their ACK/window fields describe the current receive state.
		packet.AckNr = c.send.ackNr
		packet.WindowSize = c.recv.WindowSize()
		packet.TimestampDifference = c.send.tsDifference
	}
	if packet.ConnectionID == 0 && packet.Type != Syn {
		packet.ConnectionID = c.sendID
	}
	wire, err := packet.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = c.udp.Write(wire)
	return err
}

func (c *Conn) writeResetLocked() {
	if c.udp == nil || c.send == nil {
		return
	}
	packet := Packet{
		Type:                Reset,
		ConnectionID:        c.sendID,
		Timestamp:           timestamp(time.Now()),
		TimestampDifference: c.send.tsDifference,
		SeqNr:               c.send.nextSeq,
		AckNr:               c.recv.AckNumber(),
		WindowSize:          c.recv.WindowSize(),
	}
	if wire, err := packet.MarshalBinary(); err == nil {
		_, _ = c.udp.Write(wire)
	}
}

func (c *Conn) syncSendStateLocked(now time.Time, received *Packet) {
	if c.send == nil || c.recv == nil {
		return
	}
	c.send.SetAckNumber(c.recv.AckNumber())
	c.send.SetWindowSize(c.recv.WindowSize())
	if received != nil {
		c.send.SetTimestampDifference(replyTimestamp(now, received.Timestamp))
	}
}

func (c *Conn) receiveACKLocked(now time.Time) Packet {
	return c.stampReceiveACKLocked(c.recv.AckPacket(), now)
}

func (c *Conn) stampReceiveACKLocked(ack Packet, now time.Time) Packet {
	ack.ConnectionID = c.sendID
	ack.SeqNr = c.send.nextSeq
	ack.Timestamp = timestamp(now)
	ack.TimestampDifference = c.send.tsDifference
	return ack
}

func replyTimestamp(now time.Time, received Timestamp) Timestamp {
	delta, ok := TimestampDistance(received, timestamp(now))
	if !ok {
		return 0
	}
	return Timestamp(delta)
}

func (c *Conn) setTerminalLocked(err error) {
	if err == nil {
		err = io.EOF
	}
	if c.terminal == nil {
		c.terminal = err
	}
	if c.recv != nil && c.recv.Err() == nil {
		c.recv.Cancel(err)
	}
	signal(&c.readWake)
	signal(&c.writeWake)
	signal(&c.estWake)
}

func (c *Conn) fail(err error) {
	c.mu.Lock()
	if !c.closed {
		c.setTerminalLocked(err)
	}
	c.mu.Unlock()
}

func (c *Conn) waitEstablished(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	if c.established {
		c.mu.Unlock()
		return nil
	}
	est := c.estWake
	done := c.done
	c.mu.Unlock()
	select {
	case <-est:
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.established {
			return nil
		}
		if c.terminal != nil {
			return c.terminal
		}
		return ErrNotConnected
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.established {
			return nil
		}
		if c.terminal != nil {
			return c.terminal
		}
		return ErrNotConnected
	}
}
