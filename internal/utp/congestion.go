package utp

import (
	"errors"
	"time"

	"github.com/gus-ceraso/Leech/internal/limits"
)

var ErrInvalidCongestionConfig = errors.New("utp: invalid congestion configuration")

// uTP's delay controller is deliberately kept independent of the socket. The
// sender owns one controller and feeds it the timestamp-difference value from
// each packet received from the peer. This also makes the controller useful in
// deterministic tests without a clock or a UDP socket.
const (
	defaultTargetDelay = 100 * time.Millisecond
	defaultInitialRTO  = time.Second
	minimumRTO         = 500 * time.Millisecond
	minimumPacketSize  = 150
	defaultPacketSize  = 1_200
	maximumPacketSize  = 1_400
	baseDelayWindow    = 2 * time.Minute
	delayBucketCount   = 121 // two minutes plus the current one-second bucket
)

// CongestionConfig controls the bounded delay controller. Values of zero use
// the protocol defaults. The constructor clamps packet sizes to one datagram
// and refuses values that cannot be represented safely by the sender.
type CongestionConfig struct {
	InitialWindow uint32
	InitialPacket int
	MinPacket     int
	MaxPacket     int
	TargetDelay   time.Duration
	InitialRTO    time.Duration
}

// CongestionController implements the BEP 29 delay based window controller.
// MaxWindow is a byte count, and PacketSize is a payload byte count. Both are
// local limits; the peer's advertised window is applied by SendState.
type CongestionController struct {
	maxWindow uint32
	packet    int
	minPacket int
	maxPacket int
	target    time.Duration

	baseDelay time.Duration
	hasBase   bool
	buckets   [delayBucketCount]delayBucket

	rto    time.Duration
	rtt    time.Duration
	rttVar time.Duration
	rttSet bool
}

type delayBucket struct {
	second int64
	delay  time.Duration
	valid  bool
}

// NewCongestionController creates a bounded BEP 29 controller. The zero-value
// controller is also usable after calling Reset, but callers should normally
// use this constructor.
func NewCongestionController(config CongestionConfig) (*CongestionController, error) {
	minPacket := config.MinPacket
	if minPacket == 0 {
		minPacket = minimumPacketSize
	}
	maxPacket := config.MaxPacket
	if maxPacket == 0 {
		maxPacket = maximumPacketSize
	}
	packet := config.InitialPacket
	if packet == 0 {
		packet = defaultPacketSize
	}
	if minPacket < 1 || maxPacket < minPacket || maxPacket > int(limits.DatagramBytes)-HeaderBytes || packet < minPacket || packet > maxPacket {
		return nil, ErrInvalidCongestionConfig
	}
	target := config.TargetDelay
	if target == 0 {
		target = defaultTargetDelay
	}
	if target <= 0 {
		return nil, ErrInvalidCongestionConfig
	}
	rto := config.InitialRTO
	if rto == 0 {
		rto = defaultInitialRTO
	}
	if rto < minimumRTO {
		return nil, ErrInvalidCongestionConfig
	}
	window := config.InitialWindow
	if window == 0 {
		window = uint32(packet * 2)
	}
	if window > uint32(limits.UTPBufferBytes) {
		return nil, ErrInvalidCongestionConfig
	}
	return &CongestionController{
		maxWindow: window,
		packet:    packet,
		minPacket: minPacket,
		maxPacket: maxPacket,
		target:    target,
		rto:       rto,
	}, nil
}

// NewDefaultCongestionController is the production configuration used by
// SendState.
func NewDefaultCongestionController() *CongestionController {
	controller, err := NewCongestionController(CongestionConfig{})
	if err != nil {
		panic(err)
	}
	return controller
}

// MaxWindow is the congestion window in bytes.
func (c *CongestionController) MaxWindow() uint32 {
	if c == nil {
		return 0
	}
	return c.maxWindow
}

// SetMaxWindow is a bounded test and handshake seam. It does not bypass the
// supported four-megabyte uTP buffer bound.
func (c *CongestionController) SetMaxWindow(window uint32) {
	if c == nil {
		return
	}
	if window > uint32(limits.UTPBufferBytes) {
		window = uint32(limits.UTPBufferBytes)
	}
	c.maxWindow = window
}

// PacketSize is the current payload size chosen by the delay controller.
func (c *CongestionController) PacketSize() int {
	if c == nil {
		return 0
	}
	return c.packet
}

// RTO returns the current retransmission timeout before timeout backoff.
func (c *CongestionController) RTO() time.Duration {
	if c == nil {
		return defaultInitialRTO
	}
	return c.rto
}

// BaseDelay returns the current two-minute sliding minimum. The bool is false
// until a timestamp-difference sample has been observed.
func (c *CongestionController) BaseDelay() (time.Duration, bool) {
	if c == nil || !c.hasBase {
		return 0, false
	}
	return c.baseDelay, true
}

// ObserveDelay consumes the peer's timestamp-difference measurement. The
// measurement is a one-way delay estimate relative to an unsynchronized clock,
// so only its change from the sliding minimum is meaningful. outstanding is
// the current number of payload bytes in flight.
func (c *CongestionController) ObserveDelay(now time.Time, reported time.Duration, outstanding uint32) {
	if c == nil || reported < 0 {
		return
	}
	if reported > time.Duration(^uint32(0))*time.Microsecond {
		reported = time.Duration(^uint32(0)) * time.Microsecond
	}
	second := now.UnixNano() / int64(time.Second)
	index := int(second % delayBucketCount)
	if index < 0 {
		index += delayBucketCount
	}
	bucket := &c.buckets[index]
	if !bucket.valid || bucket.second != second {
		*bucket = delayBucket{second: second, delay: reported, valid: true}
	} else if reported < bucket.delay {
		bucket.delay = reported
	}
	cutoff := second - int64(baseDelayWindow/time.Second)
	haveBase := false
	for bucketIndex := range c.buckets {
		candidate := c.buckets[bucketIndex]
		if !candidate.valid || candidate.second < cutoff || candidate.second > second {
			continue
		}
		if !haveBase || candidate.delay < c.baseDelay {
			c.baseDelay = candidate.delay
			haveBase = true
		}
	}
	if !haveBase {
		return
	}
	c.hasBase = true
	if outstanding == 0 || c.maxWindow == 0 {
		return
	}
	ourDelay := reported - c.baseDelay
	offTarget := c.target - ourDelay

	// The BEP expression scales a gain measured in packets per RTT by the
	// window utilization. One packet per observation is deliberately modest;
	// it avoids an unbounded jump when ACKs arrive in a burst while preserving
	// the required direction and byte-based window semantics.
	gain := int64(c.packet)
	gain = gain * int64(offTarget) / int64(c.target)
	gain = gain * int64(outstanding) / int64(c.maxWindow)
	if gain == 0 && offTarget != 0 {
		if offTarget > 0 {
			gain = 1
		} else {
			gain = -1
		}
	}
	window := int64(c.maxWindow) + gain
	if window < 0 {
		window = 0
	}
	if window > int64(limits.UTPBufferBytes) {
		window = int64(limits.UTPBufferBytes)
	}
	c.maxWindow = uint32(window)

	// Packet sizing follows the same signal. A congested queue quickly reaches
	// the BEP minimum; a quiet link grows in small steps toward the datagram
	// ceiling so a single delay sample cannot create a large burst.
	if ourDelay > c.target {
		c.packet = c.packet * 3 / 4
		if c.packet < c.minPacket {
			c.packet = c.minPacket
		}
	} else if c.packet < c.maxPacket {
		step := c.maxPacket / 16
		if step < 1 {
			step = 1
		}
		c.packet += step
		if c.packet > c.maxPacket {
			c.packet = c.maxPacket
		}
	}
}

// OnLoss applies BEP 29's multiplicative loss response.
func (c *CongestionController) OnLoss() {
	if c == nil {
		return
	}
	c.maxWindow /= 2
}

// OnTimeout performs the BEP 29 timeout response. SendState supplies the
// oldest packet after reducing its own timeout deadline.
func (c *CongestionController) OnTimeout() {
	if c == nil {
		return
	}
	c.packet = c.minPacket
	c.maxWindow = uint32(c.minPacket)
}

// UpdateRTT applies the BEP 29 estimator to one packet that was transmitted
// exactly once. Retransmitted packets are intentionally excluded by SendState.
func (c *CongestionController) UpdateRTT(sample time.Duration) {
	if c == nil || sample <= 0 {
		return
	}
	if sample < time.Microsecond {
		sample = time.Microsecond
	}
	if !c.hasRTT() {
		c.rtt = sample
		c.rttVar = sample / 2
		c.rttSet = true
		c.rto = c.rtt + c.rttVar*4
		if c.rto < minimumRTO {
			c.rto = minimumRTO
		}
		return
	}
	// Keep the estimator in separate fields without exposing them as tuning
	// knobs. These values are initialized lazily to avoid a second constructor
	// state for the first sample.
	delta := c.rtt - sample
	if delta < 0 {
		delta = -delta
	}
	c.rttVar += (delta - c.rttVar) / 4
	c.rtt += (sample - c.rtt) / 8
	c.rto = c.rtt + c.rttVar*4
	if c.rto < minimumRTO {
		c.rto = minimumRTO
	}
}

func (c *CongestionController) hasRTT() bool { return c.rttSet }
