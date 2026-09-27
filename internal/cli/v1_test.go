package cli

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/bencode"
	"github.com/gus-ceraso/Leech/internal/peer"
	"github.com/gus-ceraso/Leech/internal/session"
	"github.com/gus-ceraso/Leech/internal/storage"
	"github.com/gus-ceraso/Leech/internal/torrent"
	"github.com/gus-ceraso/Leech/internal/tracker"
)

const v1FixturePeerPort = 51413

type v1Tracker struct {
	mu       sync.Mutex
	urls     []string
	requests []tracker.AnnounceRequest
	host     string
	port     uint16
}

func (f *v1Tracker) Announce(ctx context.Context, rawURL string, request tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	if err := ctx.Err(); err != nil {
		return tracker.HTTPAnnounceResult{Transmitted: false}, err
	}
	f.mu.Lock()
	f.urls = append(f.urls, rawURL)
	f.requests = append(f.requests, request)
	f.mu.Unlock()
	host, port := f.host, f.port
	if host == "" {
		host = "127.0.0.1"
	}
	if port == 0 {
		port = v1FixturePeerPort
	}
	return tracker.HTTPAnnounceResult{
		Interval: time.Second, Transmitted: true,
		Peers: []tracker.HTTPPeer{{Host: host, Port: port}},
	}, nil
}

func (f *v1Tracker) snapshot() ([]string, []tracker.AnnounceRequest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.urls...), append([]tracker.AnnounceRequest(nil), f.requests...)
}

type v1UDPTracker struct {
	mu       sync.Mutex
	urls     []string
	requests []tracker.AnnounceRequest
	port     uint16
}

type v1TrackerResolver struct{}

func (v1TrackerResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	return []net.IPAddr{{IP: net.IPv4(127, 0, 0, 1)}}, nil
}

func serveV1UDPTracker(socket *net.UDPConn, requests chan<- tracker.AnnounceRequest, started chan<- struct{}) error {
	if err := socket.SetReadDeadline(time.Now().Add(7 * time.Second)); err != nil {
		return err
	}
	buffer := make([]byte, 2048)
	var connectionID uint64
	var address *net.UDPAddr
	startedReceived := false
	for {
		n, from, err := socket.ReadFromUDP(buffer)
		if err != nil {
			return err
		}
		packet := append([]byte(nil), buffer[:n]...)
		if len(packet) < 16 {
			return fmt.Errorf("short UDP tracker packet: %d", len(packet))
		}
		action := binary.BigEndian.Uint32(packet[8:12])
		tx := binary.BigEndian.Uint32(packet[12:16])
		switch action {
		case 0:
			connectionID, address = 0x0102030405060708, from
			response := make([]byte, 16)
			binary.BigEndian.PutUint32(response[:4], 0)
			binary.BigEndian.PutUint32(response[4:8], tx)
			binary.BigEndian.PutUint64(response[8:], connectionID)
			if _, err := socket.WriteToUDP(response, from); err != nil {
				return err
			}
		case 1:
			if len(packet) < 98 || address == nil || !from.IP.Equal(address.IP) || from.Port != address.Port || binary.BigEndian.Uint64(packet[:8]) != connectionID {
				return errors.New("invalid UDP tracker announce")
			}
			port := binary.BigEndian.Uint16(packet[96:98])
			event := tracker.Event(binary.BigEndian.Uint32(packet[80:84]))
			requests <- tracker.AnnounceRequest{
				Downloaded: int64(binary.BigEndian.Uint64(packet[56:64])),
				Left:       int64(binary.BigEndian.Uint64(packet[64:72])),
				Uploaded:   int64(binary.BigEndian.Uint64(packet[72:80])),
				Event:      event,
				Port:       port,
			}
			if event == tracker.EventStarted && !startedReceived {
				close(started)
				startedReceived = true
			}
			response := make([]byte, 26)
			binary.BigEndian.PutUint32(response[:4], 1)
			binary.BigEndian.PutUint32(response[4:8], tx)
			binary.BigEndian.PutUint32(response[8:12], 60)
			binary.BigEndian.PutUint32(response[12:16], 1)
			binary.BigEndian.PutUint32(response[16:20], 0)
			copy(response[20:24], []byte{127, 0, 0, 1})
			binary.BigEndian.PutUint16(response[24:26], port)
			if _, err := socket.WriteToUDP(response, from); err != nil {
				return err
			}
			if tracker.Event(binary.BigEndian.Uint32(packet[80:84])) == tracker.EventStopped {
				return nil
			}
		default:
			return fmt.Errorf("unexpected UDP tracker action %d", action)
		}
	}
}

func (f *v1UDPTracker) Announce(ctx context.Context, rawURL string, request tracker.AnnounceRequest) (tracker.AnnounceResult, error) {
	if err := ctx.Err(); err != nil {
		return tracker.AnnounceResult{}, err
	}
	f.mu.Lock()
	f.urls = append(f.urls, rawURL)
	f.requests = append(f.requests, request)
	f.mu.Unlock()
	return tracker.AnnounceResult{
		Interval: time.Second, Transmitted: true,
		Peers: []netip.AddrPort{netip.MustParseAddrPort(net.JoinHostPort("127.0.0.1", fmt.Sprint(f.port)))},
	}, nil
}

func (f *v1UDPTracker) snapshot() ([]string, []tracker.AnnounceRequest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.urls...), append([]tracker.AnnounceRequest(nil), f.requests...)
}

func TestV1KnownTorrentRunsThroughCLIAndOverwritesSelectedOutput(t *testing.T) {
	data := []byte("verified CLI payload")
	infoBytes, infoHash := v1Info(t, data)
	torrentPath := writeV1Torrent(t, v1Metainfo(t, infoBytes, "http://input.fixture/announce"))
	output := t.TempDir()
	if err := os.WriteFile(filepath.Join(output, "payload.bin"), []byte("stale bytes that must be truncated"), 0o600); err != nil {
		t.Fatal(err)
	}
	trackerFixture := &v1Tracker{}
	peers := newV1Peers(t, infoHash, infoBytes, data, false)

	opts := parseV1Options(t, "--output", output, "--stream", torrentPath)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	err := RunWithSession(ctx, opts, &bytes.Buffer{}, &bytes.Buffer{}, v1SessionConfig(t, trackerFixture, peers))
	if err != nil {
		t.Fatalf("CLI run: %v", err)
	}
	if err := peers.wait(t); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(output, "payload.bin"))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("output = %q, %v; want %q", got, err, data)
	}
	urls, requests := trackerFixture.snapshot()
	assertV1TrackerTrace(t, urls, requests, []string{"http://input.fixture/announce", torrent.DefaultTracker}, int64(len(data)), false)
	peers.assertNoUpload(t)
}

func TestV1UDPTrackerPeerCompletesCLITransfer(t *testing.T) {
	data := []byte("UDP discovery")
	infoBytes, infoHash := v1Info(t, data)
	udpURL := "udp://fixture.test:51414/announce"
	torrentPath := writeV1Torrent(t, v1Metainfo(t, infoBytes, udpURL))
	output := t.TempDir()
	httpFixture := &v1Tracker{}
	udpFixture := &v1UDPTracker{port: v1FixturePeerPort}
	peers := newV1Peers(t, infoHash, infoBytes, data, false)
	defer peers.close()
	config := v1SessionConfig(t, httpFixture, peers)
	config.UDP = udpFixture
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	opts := parseV1Options(t, "--output", output, torrentPath)
	if err := RunWithSession(ctx, opts, &bytes.Buffer{}, &bytes.Buffer{}, config); err != nil {
		t.Fatalf("CLI run via UDP tracker: %v", err)
	}
	if err := peers.wait(t); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(output, "payload.bin")); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("output=%q err=%v", got, err)
	}
	httpURLs, httpRequests := httpFixture.snapshot()
	if !hasV1String(httpURLs, torrent.DefaultTracker) {
		t.Fatalf("mandatory default HTTP tracker not used: %v", httpURLs)
	}
	udpURLs, udpRequests := udpFixture.snapshot()
	if !hasV1String(udpURLs, udpURL) {
		t.Fatalf("input UDP tracker not used: %v", udpURLs)
	}
	assertV1TrackerTrace(t, append(httpURLs, udpURLs...), append(httpRequests, udpRequests...), []string{torrent.DefaultTracker, udpURL}, int64(len(data)), false)
	peers.assertNoUpload(t)
}

func TestV1UDPTrackerWirePathCompletesCLITransfer(t *testing.T) {
	trackerSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer trackerSocket.Close()
	data := []byte("UDP wire discovery")
	infoBytes, infoHash := v1Info(t, data)
	udpURL := "udp://fixture.test:" + fmt.Sprint(trackerSocket.LocalAddr().(*net.UDPAddr).Port) + "/announce"
	torrentPath := writeV1Torrent(t, v1Metainfo(t, infoBytes, udpURL))
	output := t.TempDir()
	httpFixture := &v1Tracker{}
	peers := newV1Peers(t, infoHash, infoBytes, data, false)
	defer peers.close()
	serverDone := make(chan error, 1)
	udpRequests := make(chan tracker.AnnounceRequest, 8)
	started := make(chan struct{})
	go func() { serverDone <- serveV1UDPTracker(trackerSocket, udpRequests, started) }()
	config := v1SessionConfig(t, httpFixture, peers)
	config.UDP = tracker.NewUDPClient(tracker.Config{Resolver: v1TrackerResolver{}})
	config.TCPDial = func(ctx context.Context, network, address string) (net.Conn, error) {
		select {
		case <-started:
			return peers.dial(ctx, network, address)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	opts := parseV1Options(t, "--output", output, torrentPath)
	if err := RunWithSession(ctx, opts, &bytes.Buffer{}, &bytes.Buffer{}, config); err != nil {
		t.Fatalf("CLI run via UDP tracker wire path: %v", err)
	}
	if err := peers.wait(t); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	default:
		t.Fatal("UDP tracker did not receive started before peer transfer")
	}
	if got, err := os.ReadFile(filepath.Join(output, "payload.bin")); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("output=%q err=%v", got, err)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("UDP tracker fixture: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("UDP tracker fixture was not joined")
	}
	urls, requests := httpFixture.snapshot()
	if !hasV1String(urls, torrent.DefaultTracker) {
		t.Fatalf("mandatory default HTTP tracker not used: %v", urls)
	}
	var udpTrace []tracker.AnnounceRequest
	for {
		select {
		case request := <-udpRequests:
			udpTrace = append(udpTrace, request)
		default:
			goto udpTraceCollected
		}
	}
udpTraceCollected:
	udpURLs := make([]string, len(udpTrace))
	for i := range udpURLs {
		udpURLs[i] = udpURL
	}
	assertV1TrackerTrace(t, append(urls, udpURLs...), append(requests, udpTrace...), []string{torrent.DefaultTracker, udpURL}, int64(len(data)), false)
	peers.assertNoUpload(t)
}

func TestV1IPv6LoopbackPeerCompletesTCPTransfer(t *testing.T) {
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Fatalf("IPv6 loopback listener unavailable: %v", err)
	}
	defer listener.Close()
	port := uint16(listener.Addr().(*net.TCPAddr).Port)
	data := []byte("IPv6 endpoint")
	infoBytes, infoHash := v1Info(t, data)
	torrentPath := writeV1Torrent(t, v1Metainfo(t, infoBytes, ""))
	output := t.TempDir()
	trackerFixture := &v1Tracker{host: "::1", port: port}
	peerFixture := newV1Peers(t, infoHash, infoBytes, data, false)
	serverDone := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverDone <- acceptErr
			return
		}
		peerFixture.calls.Add(1)
		serverDone <- peerFixture.serveTransfer(conn)
	}()
	config := v1SessionConfig(t, trackerFixture, peerFixture)
	config.TCPDial = (&net.Dialer{}).DialContext
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	opts := parseV1Options(t, "--output", output, torrentPath)
	if err := RunWithSession(ctx, opts, &bytes.Buffer{}, &bytes.Buffer{}, config); err != nil {
		t.Fatalf("IPv6 CLI transfer: %v", err)
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("IPv6 peer fixture: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("IPv6 peer was not joined")
	}
	if got, err := os.ReadFile(filepath.Join(output, "payload.bin")); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("IPv6 output=%q err=%v", got, err)
	}
	urls, requests := trackerFixture.snapshot()
	assertV1TrackerTrace(t, urls, requests, []string{torrent.DefaultTracker}, int64(len(data)), false)
	peerFixture.assertNoUpload(t)
}

func TestV1SelectiveCLIWritesOnlySelectedPathAndStopsWithoutCompletion(t *testing.T) {
	data := []byte("oneTWO!")
	infoBytes, infoHash := v1MultiInfo(t, data)
	torrentPath := writeV1Torrent(t, v1Metainfo(t, infoBytes, ""))
	output := t.TempDir()
	trackerFixture := &v1Tracker{}
	peers := newV1Peers(t, infoHash, infoBytes, data, false)
	defer peers.close()

	opts := parseV1Options(t, "--output", output, "--file", "two.bin", torrentPath)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := RunWithSession(ctx, opts, &bytes.Buffer{}, &bytes.Buffer{}, v1SessionConfig(t, trackerFixture, peers)); err != nil {
		select {
		case peerErr := <-peers.done:
			t.Fatalf("CLI selective run: %v; peer fixture: %v; calls=%d", err, peerErr, peers.calls.Load())
		default:
			t.Fatalf("CLI selective run: %v; no peer fixture result; calls=%d", err, peers.calls.Load())
		}
	}
	if err := peers.wait(t); err != nil {
		t.Fatal(err)
	}
	selected, err := os.ReadFile(filepath.Join(output, "release", "two.bin"))
	if err != nil || string(selected) != "TWO!" {
		t.Fatalf("selected output = %q, %v", selected, err)
	}
	if _, err := os.Lstat(filepath.Join(output, "release", "one.bin")); !os.IsNotExist(err) {
		t.Fatalf("unselected path was created: %v", err)
	}
	urls, requests := trackerFixture.snapshot()
	if len(urls) == 0 || !hasV1String(urls, torrent.DefaultTracker) {
		t.Fatalf("default tracker was not routed to fixture: %v", urls)
	}
	started, stopped, completed := 0, 0, 0
	for i, request := range requests {
		if request.Uploaded != 0 {
			t.Errorf("tracker request %d = %+v, want uploaded=0", i, request)
		}
		if request.Event == tracker.EventStarted && request.Left != int64(len(data)) {
			t.Errorf("started request %d reported left=%d, want entire torrent size %d", i, request.Left, len(data))
		}
		switch request.Event {
		case tracker.EventStarted:
			started++
		case tracker.EventStopped:
			stopped++
		case tracker.EventCompleted:
			completed++
		}
	}
	if started != 1 || stopped != 1 || completed != 0 {
		t.Fatalf("selected-only tracker events = started:%d completed:%d stopped:%d", started, completed, stopped)
	}
	peers.assertNoUpload(t)
}

func TestV1CLIReportsVerifiedPieceAfterLaterFailure(t *testing.T) {
	data := bytes.Repeat([]byte("x"), 32<<10)
	infoBytes, infoHash := v1TwoPieceInfo(t, data)
	torrentPath := writeV1Torrent(t, v1Metainfo(t, infoBytes, ""))
	output := t.TempDir()
	trackerFixture := &v1Tracker{}
	peers := newV1Peers(t, infoHash, infoBytes, data, false)
	peers.pieceLength = 16 << 10
	defer peers.close()
	opts := parseV1Options(t, "--output", output, "--timeout", "150ms", torrentPath)
	var stderr bytes.Buffer
	err := RunWithSession(context.Background(), opts, &bytes.Buffer{}, &stderr, v1SessionConfig(t, trackerFixture, peers))
	if err == nil {
		t.Fatal("run unexpectedly completed despite the second piece being unavailable")
	}
	if !strings.Contains(stderr.String(), "verified partial output remains resumable") {
		t.Fatalf("failure omitted resumable output status: %q", stderr.String())
	}
	got, readErr := os.ReadFile(filepath.Join(output, "payload.bin"))
	if readErr != nil || !bytes.Equal(got, data[:16<<10]) {
		t.Fatalf("verified output = %d bytes, %v; want first 16 KiB", len(got), readErr)
	}
}

func TestV1CLIReportsVerifiedResumePieceAfterLaterFailure(t *testing.T) {
	data := bytes.Repeat([]byte("r"), 32<<10)
	infoBytes, infoHash := v1TwoPieceInfo(t, data)
	torrentPath := writeV1Torrent(t, v1Metainfo(t, infoBytes, ""))
	output := t.TempDir()
	if err := os.WriteFile(filepath.Join(output, "payload.bin"), data[:16<<10], 0o600); err != nil {
		t.Fatal(err)
	}
	trackerFixture := &v1Tracker{}
	peers := newV1Peers(t, infoHash, infoBytes, data, false)
	peers.pieceLength = 16 << 10
	defer peers.close()
	opts := parseV1Options(t, "--output", output, "--resume", "--timeout", "150ms", torrentPath)
	var stderr bytes.Buffer
	err := RunWithSession(context.Background(), opts, &bytes.Buffer{}, &stderr, v1SessionConfig(t, trackerFixture, peers))
	if err == nil {
		t.Fatal("run unexpectedly completed despite the second piece being unavailable")
	}
	if !strings.Contains(stderr.String(), "verified partial output remains resumable") {
		t.Fatalf("failure omitted verified resume output status: %q", stderr.String())
	}
	got, readErr := os.ReadFile(filepath.Join(output, "payload.bin"))
	if readErr != nil || !bytes.Equal(got, data[:16<<10]) {
		t.Fatalf("resumed output = %d bytes, %v; want first 16 KiB", len(got), readErr)
	}
}

func TestV1CLIResumePreservesVerifiedPieceAndWritesMissingPiece(t *testing.T) {
	data := bytes.Repeat([]byte("s"), 32<<10)
	infoBytes, infoHash := v1TwoPieceInfo(t, data)
	torrentPath := writeV1Torrent(t, v1Metainfo(t, infoBytes, ""))
	output := t.TempDir()
	if err := os.WriteFile(filepath.Join(output, "payload.bin"), data[:16<<10], 0o600); err != nil {
		t.Fatal(err)
	}
	trackerFixture := &v1Tracker{}
	peers := newV1Peers(t, infoHash, infoBytes, data, false)
	peers.pieceLength = 16 << 10
	peers.bitfield = []byte{0xc0}
	defer peers.close()
	opts := parseV1Options(t, "--output", output, "--resume", torrentPath)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := RunWithSession(ctx, opts, &bytes.Buffer{}, &bytes.Buffer{}, v1SessionConfig(t, trackerFixture, peers)); err != nil {
		t.Fatalf("CLI resume: %v", err)
	}
	if got, err := os.ReadFile(filepath.Join(output, "payload.bin")); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("resumed output = %d bytes, %v; want all %d bytes", len(got), err, len(data))
	}
	if err := peers.wait(t); err != nil {
		t.Fatal(err)
	}
	peers.mu.Lock()
	requested := append([]uint32(nil), peers.requestedPieces...)
	peers.mu.Unlock()
	if len(requested) == 0 {
		t.Fatal("peer received no block requests")
	}
	for _, index := range requested {
		if index != 1 {
			t.Fatalf("requested piece indices = %v, want missing piece 1 only", requested)
		}
	}
}

func TestV1CLIWithoutResumeTruncatesExistingOutputBeforeTransfer(t *testing.T) {
	data := []byte("do not retain")
	infoBytes, infoHash := v1Info(t, data)
	torrentPath := writeV1Torrent(t, v1Metainfo(t, infoBytes, ""))
	output := t.TempDir()
	outputPath := filepath.Join(output, "payload.bin")
	if err := os.WriteFile(outputPath, []byte("old output"), 0o600); err != nil {
		t.Fatal(err)
	}
	trackerFixture := &v1Tracker{}
	peers := newV1Peers(t, infoHash, infoBytes, data, false)
	peers.stall = true
	defer peers.close()
	opts := parseV1Options(t, "--output", output, "--timeout", "150ms", torrentPath)
	err := RunWithSession(context.Background(), opts, &bytes.Buffer{}, &bytes.Buffer{}, v1SessionConfig(t, trackerFixture, peers))
	if err == nil {
		t.Fatal("stalled transfer unexpectedly succeeded")
	}
	got, readErr := os.ReadFile(outputPath)
	if readErr != nil || len(got) != 0 {
		t.Fatalf("output after overwrite-mode failure = %q, %v; want empty file", got, readErr)
	}
}

func TestV1CLICompleteResumeSkipsNetwork(t *testing.T) {
	data := []byte("already verified")
	infoBytes, _ := v1Info(t, data)
	torrentPath := writeV1Torrent(t, v1Metainfo(t, infoBytes, ""))
	output := t.TempDir()
	if err := os.WriteFile(filepath.Join(output, "payload.bin"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	trackerFixture := &v1Tracker{}
	peers := newV1Peers(t, [20]byte{}, nil, nil, false)
	opts := parseV1Options(t, "--output", output, "--resume", torrentPath)
	if err := RunWithSession(context.Background(), opts, &bytes.Buffer{}, &bytes.Buffer{}, v1SessionConfig(t, trackerFixture, peers)); err != nil {
		t.Fatalf("CLI resume: %v", err)
	}
	urls, requests := trackerFixture.snapshot()
	if len(urls) != 0 || len(requests) != 0 || peers.calls.Load() != 0 {
		t.Fatalf("complete resume used network: trackers=%d dials=%d", len(requests), peers.calls.Load())
	}
}

func TestV1CLIResumeRedownloadsIncompletePiece(t *testing.T) {
	data := []byte("partial resume piece")
	infoBytes, infoHash := v1Info(t, data)
	torrentPath := writeV1Torrent(t, v1Metainfo(t, infoBytes, ""))
	output := t.TempDir()
	if err := os.WriteFile(filepath.Join(output, "payload.bin"), data[:7], 0o600); err != nil {
		t.Fatal(err)
	}
	trackerFixture := &v1Tracker{}
	peers := newV1Peers(t, infoHash, infoBytes, data, false)
	defer peers.close()
	opts := parseV1Options(t, "--output", output, "--resume", torrentPath)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := RunWithSession(ctx, opts, &bytes.Buffer{}, &bytes.Buffer{}, v1SessionConfig(t, trackerFixture, peers)); err != nil {
		t.Fatalf("CLI partial resume: %v", err)
	}
	if err := peers.wait(t); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(output, "payload.bin")); err != nil || !bytes.Equal(got, data) {
		t.Fatalf("resumed output = %q, %v; want %q", got, err, data)
	}
	if !peers.requestedFromStart(t) {
		t.Error("resume did not redownload the incomplete piece from offset zero")
	}
	urls, requests := trackerFixture.snapshot()
	assertV1TrackerTrace(t, urls, requests, []string{torrent.DefaultTracker}, int64(len(data)), false)
	peers.assertNoUpload(t)
}

func TestV1CLIMixedPieceResumeRequestsWholePieceAndRetainsSelectedBytes(t *testing.T) {
	data := []byte("ABCD")
	infoBytes, infoHash := v1TwoFilePieceInfo(t, data)
	torrentPath := writeV1Torrent(t, v1Metainfo(t, infoBytes, ""))
	output := t.TempDir()
	if err := os.Mkdir(filepath.Join(output, "release"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(output, "release", "A"), []byte("AB"), 0o600); err != nil {
		t.Fatal(err)
	}
	trackerFixture := &v1Tracker{}
	peers := newV1Peers(t, infoHash, infoBytes, data, false)
	defer peers.close()
	opts := parseV1Options(t, "--output", output, "--file", "A", "--resume", torrentPath)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := RunWithSession(ctx, opts, &bytes.Buffer{}, &bytes.Buffer{}, v1SessionConfig(t, trackerFixture, peers)); err != nil {
		t.Fatalf("CLI mixed-piece resume: %v", err)
	}
	if err := peers.wait(t); err != nil {
		t.Fatal(err)
	}
	peers.mu.Lock()
	requested := append([][2]uint32(nil), peers.requested...)
	peers.mu.Unlock()
	var requestedBytes uint32
	for _, block := range requested {
		requestedBytes += block[1]
	}
	if requestedBytes != uint32(len(data)) {
		t.Fatalf("requested block ranges %v cover %d bytes, want full %d-byte piece", requested, requestedBytes, len(data))
	}
	if got, err := os.ReadFile(filepath.Join(output, "release", "A")); err != nil || string(got) != "AB" {
		t.Fatalf("selected A = %q, %v; want AB", got, err)
	}
	if _, err := os.Stat(filepath.Join(output, "release", "B")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unselected B exists: %v", err)
	}
	urls, requests := trackerFixture.snapshot()
	if len(urls) != len(requests) || len(requests) != 2 {
		t.Fatalf("tracker trace lengths = %d/%d, want one started and one stopped", len(urls), len(requests))
	}
	if requests[0].Event != tracker.EventStarted || requests[0].Left != 4 || requests[1].Event != tracker.EventStopped || requests[1].Left != 2 {
		t.Fatalf("tracker requests = %+v, want started left=4 then stopped left=2", requests)
	}
	for _, request := range requests {
		if request.Event == tracker.EventCompleted {
			t.Fatal("partial selection sent completed")
		}
		if request.Uploaded != 0 {
			t.Fatalf("tracker reported uploaded=%d", request.Uploaded)
		}
	}
	peers.assertNoUpload(t)
}

func TestV1CLITimeoutCleansPieceWorkspace(t *testing.T) {
	data := []byte("must remain incomplete")
	infoBytes, infoHash := v1Info(t, data)
	torrentPath := writeV1Torrent(t, v1Metainfo(t, infoBytes, ""))
	output, cacheRoot := t.TempDir(), filepath.Join(t.TempDir(), "cache")
	trackerFixture := &v1Tracker{}
	peerFixture := newV1Peers(t, infoHash, infoBytes, data, false)
	peerFixture.stall = true
	config := v1SessionConfig(t, trackerFixture, peerFixture)
	var dialCount atomic.Int32
	config.TCPDial = func(ctx context.Context, network, address string) (net.Conn, error) {
		if dialCount.Add(1) > 1 {
			return nil, errors.New("fixture allows only one timed-out connection")
		}
		return peerFixture.dial(ctx, network, address)
	}
	config.CacheRoot = cacheRoot
	opts := parseV1Options(t, "--output", output, "--timeout", "100ms", torrentPath)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := RunWithSession(ctx, opts, &bytes.Buffer{}, &bytes.Buffer{}, config); err == nil {
		t.Fatal("CLI no-progress timeout unexpectedly succeeded")
	}
	peerFixture.close()
	if err := peerFixture.wait(t); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(cacheRoot)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("timeout left cache workspace entries: %v", entries)
	}
	urls, requests := trackerFixture.snapshot()
	assertV1StoppedTrace(t, urls, requests, torrent.DefaultTracker, int64(len(data)))
}

func TestV1CLITimeoutReportsStagedCloseFailureSeparately(t *testing.T) {
	data := []byte("withheld after request")
	infoBytes, infoHash := v1Info(t, data)
	torrentPath := writeV1Torrent(t, v1Metainfo(t, infoBytes, ""))
	output, cacheRoot := t.TempDir(), filepath.Join(t.TempDir(), "cache")
	trackerFixture := &v1Tracker{}
	peers := newV1Peers(t, infoHash, infoBytes, data, false)
	peers.withhold = true
	defer peers.close()
	config := v1SessionConfig(t, trackerFixture, peers)
	config.CacheRoot = cacheRoot
	closeErr := errors.New("injected staged close failure")
	stageOpened := make(chan struct{})
	config.StageFileOpener = func(path string, flag int, mode os.FileMode) (storage.StagingFile, error) {
		file, err := os.OpenFile(path, flag, mode)
		if err != nil {
			return nil, err
		}
		close(stageOpened)
		return v1CloseFailureStageFile{File: file, err: closeErr}, nil
	}
	opts := parseV1Options(t, "--output", output, "--timeout", "500ms", "--loglevel", "error", torrentPath)
	var stderr bytes.Buffer
	runDone := make(chan error, 1)
	go func() { runDone <- RunWithSession(context.Background(), opts, &bytes.Buffer{}, &stderr, config) }()
	select {
	case <-stageOpened:
	case <-time.After(time.Second):
		t.Fatal("stage was not opened before timeout")
	}
	var runErr error
	select {
	case runErr = <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("CLI did not finish after timeout")
	}
	if !errors.Is(runErr, session.ErrNoProgressTimeout) {
		t.Fatalf("CLI error = %v, want no-progress timeout", runErr)
	}
	peers.close()
	if err := peers.wait(t); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("diagnostics = %q, want one primary and one secondary line", stderr.String())
	}
	var primary, cleanup bool
	for _, line := range lines {
		if strings.Contains(line, "failure: no verified file pieces before timeout") {
			primary = true
			if strings.Contains(line, closeErr.Error()) {
				t.Errorf("primary timeout diagnostic includes cleanup failure: %q", line)
			}
		}
		if strings.Contains(line, "shutdown: ") && strings.Contains(line, closeErr.Error()) {
			cleanup = true
		}
		if !strings.HasPrefix(line, "error: ") {
			t.Errorf("diagnostic = %q, want error-level prefix", line)
		}
	}
	if !primary || !cleanup {
		t.Fatalf("diagnostics = %q, want distinct timeout and cleanup failures", stderr.String())
	}
}

type v1CloseFailureStageFile struct {
	*os.File
	err error
}

func (f v1CloseFailureStageFile) Close() error {
	_ = f.File.Close()
	return f.err
}

type v1FinalFailureTracker struct {
	delegate tracker.TrackerHTTP
	err      error
}

func (f v1FinalFailureTracker) Announce(ctx context.Context, rawURL string, request tracker.AnnounceRequest) (tracker.HTTPAnnounceResult, error) {
	if request.Event == tracker.EventStopped {
		return tracker.HTTPAnnounceResult{Transmitted: true}, f.err
	}
	return f.delegate.Announce(ctx, rawURL, request)
}

func TestV1CLIPrimaryAndSecondaryErrorsRedactBeforeTruncation(t *testing.T) {
	data := []byte("redaction boundary")
	info, infoHash := v1Info(t, data)
	torrentPath := writeV1Torrent(t, v1Metainfo(t, info, ""))
	peers := newV1Peers(t, infoHash, info, data, false)
	defer peers.close()
	delegate := &v1Tracker{}
	secondaryText := "tracker https://secondary-user:secondary-password@[2001:db8::22]:9443/private'path?token=secondary-query-secret " +
		"magnet:?xt=urn:btih:0123456789012345678901234567890123456789&dn=secondary-magnet-secret\x1b[31m"
	config := v1SessionConfig(t, delegate, peers)
	config.HTTP = v1FinalFailureTracker{delegate: delegate, err: errors.New(secondaryText)}
	userinfo := strings.Repeat("primary-userinfo-secret-", 3000)
	primaryText := "staged read failed: https://" + userinfo + "@[2001:db8::11]:8443/primary-private'path?token=primary-query-secret\x1b[31m"
	primaryErr := errors.New(primaryText)
	config.StageFileOpener = func(path string, flag int, mode os.FileMode) (storage.StagingFile, error) {
		file, err := os.OpenFile(path, flag, mode)
		if err != nil {
			return nil, err
		}
		return v1ReadFailureStageFile{File: file, err: primaryErr}, nil
	}
	var stdout, stderr bytes.Buffer
	opts := parseV1Options(t, "--output", t.TempDir(), "--loglevel", "error", torrentPath)
	runErr := RunWithSession(context.Background(), opts, &stdout, &stderr, config)
	if !errors.Is(runErr, primaryErr) {
		t.Fatalf("session error = %v, want staged read failure", runErr)
	}
	if err := peers.wait(t); err != nil {
		t.Fatal(err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("errors wrote stdout: %q", stdout.String())
	}
	lines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("diagnostics = %q, want primary and secondary lines", stderr.String())
	}
	for _, secret := range []string{
		"primary-userinfo-secret", "primary-private", "primary-query-secret",
		"secondary-user", "secondary-password", "/private", "secondary-query-secret",
		"magnet:?", "0123456789012345678901234567890123456789", "secondary-magnet-secret", "\x1b",
	} {
		if strings.Contains(stderr.String(), secret) {
			t.Errorf("CLI diagnostics leaked %q: %q", secret, stderr.String())
		}
	}
	for _, host := range []string{"[2001:db8::11]:8443", "[2001:db8::22]:9443"} {
		if !strings.Contains(stderr.String(), host) {
			t.Errorf("CLI diagnostics lost IPv6 host %q: %q", host, stderr.String())
		}
	}
	for _, line := range lines {
		if len(line) > DefaultDiagnosticBytes+len("error: ")+1 {
			t.Errorf("rendered error line is unbounded (%d bytes): %q", len(line), line)
		}
	}
	primaryLine, secondaryLine := false, false
	for _, line := range lines {
		primaryLine = primaryLine || strings.HasPrefix(line, "error: failure: ")
		secondaryLine = secondaryLine || strings.HasPrefix(line, "error: shutdown: ")
	}
	if !primaryLine || !secondaryLine {
		t.Fatalf("primary and secondary classifications changed: %q", lines)
	}
	if !strings.Contains(stderr.String(), `\x1b[31m`) {
		t.Fatalf("control characters were not escaped: %q", stderr.String())
	}
}

func TestV1CLIStagedReadFailureStopsAndCleansSession(t *testing.T) {
	data := []byte("fail staged read")
	infoBytes, infoHash := v1Info(t, data)
	torrentPath := writeV1Torrent(t, v1Metainfo(t, infoBytes, ""))
	output, cacheRoot := t.TempDir(), filepath.Join(t.TempDir(), "cache")
	trackerFixture := &v1Tracker{}
	peers := newV1Peers(t, infoHash, infoBytes, data, false)
	defer peers.close()
	readErr := errors.New("injected staged read failure")
	config := v1SessionConfig(t, trackerFixture, peers)
	config.CacheRoot = cacheRoot
	config.StageFileOpener = func(path string, flag int, mode os.FileMode) (storage.StagingFile, error) {
		file, err := os.OpenFile(path, flag, mode)
		if err != nil {
			return nil, err
		}
		return v1ReadFailureStageFile{File: file, err: readErr}, nil
	}
	opts := parseV1Options(t, "--output", output, torrentPath)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := RunWithSession(ctx, opts, &bytes.Buffer{}, &bytes.Buffer{}, config)
	if !errors.Is(err, readErr) {
		t.Fatalf("session error = %v, want staged read failure", err)
	}
	if err := peers.wait(t); err != nil {
		t.Fatal(err)
	}
	urls, requests := trackerFixture.snapshot()
	assertV1StoppedTrace(t, urls, requests, torrent.DefaultTracker, int64(len(data)))
	for _, request := range requests {
		if request.Event == tracker.EventCompleted {
			t.Fatal("staged read failure sent completed")
		}
	}
	entries, err := os.ReadDir(cacheRoot)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("staged read failure left cache workspace: %v", entries)
	}
	peers.assertNoUpload(t)
}

type v1ReadFailureStageFile struct {
	*os.File
	err error
}

func (f v1ReadFailureStageFile) ReadAt([]byte, int64) (int, error) { return 0, f.err }

func assertV1StoppedTrace(t *testing.T, urls []string, requests []tracker.AnnounceRequest, wantURL string, fullLeft int64) {
	t.Helper()
	seen := make(map[string][]tracker.AnnounceRequest)
	for i, rawURL := range urls {
		if i >= len(requests) {
			t.Fatalf("tracker URL/request trace lengths differ: %d/%d", len(urls), len(requests))
		}
		request := requests[i]
		if request.Uploaded != 0 {
			t.Errorf("announce to %s reported uploaded=%d", rawURL, request.Uploaded)
		}
		seen[rawURL] = append(seen[rawURL], request)
	}
	trace := seen[wantURL]
	if !hasEventSequence(eventsOf(trace), tracker.EventStarted, tracker.EventStopped) {
		t.Fatalf("tracker %s missing started/stopped timeout trace: %+v", wantURL, trace)
	}
	if len(trace) < 2 || trace[0].Left != fullLeft {
		t.Errorf("timeout started left=%d, want full torrent size %d", trace[0].Left, fullLeft)
	}
}

func eventsOf(requests []tracker.AnnounceRequest) []tracker.Event {
	events := make([]tracker.Event, len(requests))
	for i := range requests {
		events[i] = requests[i].Event
	}
	return events
}

func TestV1RemoteListingStopsAfterMetadataWithoutCreatingCache(t *testing.T) {
	data := []byte("metadata only")
	infoBytes, infoHash := v1Info(t, data)
	trackerFixture := &v1Tracker{}
	peers := newV1Peers(t, infoHash, infoBytes, data, true)
	config := v1SessionConfig(t, trackerFixture, peers)
	cache := config.CacheRoot
	source := "magnet:?xt=urn:btih:" + hex.EncodeToString(infoHash[:])
	opts := Options{Source: source, ListFiles: true, LogLevel: LogError}
	var stdout bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := RunWithSession(ctx, opts, &stdout, &bytes.Buffer{}, config); err != nil {
		t.Fatalf("remote listing: %v", err)
	}
	if got, want := stdout.String(), "\"payload.bin\"\n"; got != want {
		t.Fatalf("remote listing = %q, want %q", got, want)
	}
	if peers.calls.Load() != 1 {
		t.Fatalf("remote listing opened %d peer connections, want metadata-only one", peers.calls.Load())
	}
	select {
	case err := <-peers.done:
		if err != nil {
			t.Fatalf("metadata peer fixture: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("metadata peer was not joined")
	}
	if _, err := os.Lstat(cache); !os.IsNotExist(err) {
		t.Fatalf("remote listing created piece cache %q: %v", cache, err)
	}
	urls, requests := trackerFixture.snapshot()
	if !hasV1String(urls, torrent.DefaultTracker) {
		t.Fatalf("mandatory default tracker was not called: %v", urls)
	}
	started, stopped := 0, 0
	for _, request := range requests {
		if request.Uploaded != 0 || request.Left != 1 {
			t.Errorf("metadata listing announce = %+v, want left=1 and uploaded=0", request)
		}
		if request.Event == tracker.EventStarted {
			started++
		}
		if request.Event == tracker.EventStopped {
			stopped++
		}
	}
	if started != 1 || stopped != 1 {
		t.Fatalf("remote listing tracker events: started=%d stopped=%d", started, stopped)
	}
}

func TestV1MagnetAndBareHashRunMetadataThenTransferThroughCLI(t *testing.T) {
	data := []byte("metadata then transfer")
	infoBytes, infoHash := v1Info(t, data)
	hash := hex.EncodeToString(infoHash[:])
	sources := []struct {
		name   string
		source string
	}{
		{name: "magnet", source: "magnet:?xt=urn:btih:" + hash + "&x.pe=127.0.0.1%3A51413"},
		{name: "bare hash", source: hash},
	}
	for _, test := range sources {
		t.Run(test.name, func(t *testing.T) {
			output := t.TempDir()
			trackerFixture := &v1Tracker{}
			peers := newV1Peers(t, infoHash, infoBytes, data, true)
			opts := parseV1Options(t, "--loglevel", "debug", "--output", output, test.source)
			config := v1SessionConfig(t, trackerFixture, peers)
			var phases []string
			var races []session.Diagnostic
			config.OnPhase = func(phase string) { phases = append(phases, phase) }
			config.OnDiagnostic = func(event session.Diagnostic) {
				if event.Kind == session.DiagnosticTransportRace {
					races = append(races, event)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			var stdout, stderr bytes.Buffer
			err := RunWithSession(ctx, opts, &stdout, &stderr, config)
			if err != nil {
				t.Fatalf("CLI run: %v", err)
			}
			if got := strings.Join(phases, ","); got != "metadata,selection,transfer" {
				t.Fatalf("CLI phases = %q, want metadata,selection,transfer", got)
			}
			metadataWinner, transferWinner := false, false
			for _, event := range races {
				if !strings.HasPrefix(event.Detail, "winner=") {
					continue
				}
				metadataWinner = metadataWinner || event.Phase == "metadata"
				transferWinner = transferWinner || event.Phase == "transfer"
			}
			if !metadataWinner || !transferWinner {
				t.Fatalf("transport race winners metadata=%t transfer=%t: %+v", metadataWinner, transferWinner, races)
			}
			for _, diagnostic := range []string{"debug: peer selection phase=metadata", "debug: transfer", "piece 0 assigned", "piece 0 completed", "first useful block accepted and staged", "debug: session lifecycle"} {
				if !strings.Contains(stderr.String(), diagnostic) {
					t.Fatalf("production %s diagnostic missing: %q", diagnostic, stderr.String())
				}
			}
			if stdout.Len() != 0 {
				t.Fatalf("runtime diagnostics wrote to stdout: %q", stdout.String())
			}
			if err := peers.wait(t); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(filepath.Join(output, "payload.bin"))
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("output = %q, %v; want %q", got, err, data)
			}
			urls, requests := trackerFixture.snapshot()
			// An embedded magnet peer can finish metadata before the tracker
			// announces. A bare hash needs the tracker to find its supplier.
			assertV1TrackerTrace(t, urls, requests, []string{torrent.DefaultTracker}, int64(len(data)), test.name == "bare hash")
			peers.assertNoUpload(t)
		})
	}
}

func v1SessionConfig(t *testing.T, fixture *v1Tracker, peers *v1PeerFixture) session.RunConfig {
	t.Helper()
	return session.RunConfig{
		HTTP:    fixture,
		TCPDial: peers.dial,
		UTPDial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
		UTPHeadStart: time.Millisecond,
		Identity:     tracker.Identity{PeerID: [20]byte{0x51, 0x31}, Port: 49152},
		CacheRoot:    filepath.Join(t.TempDir(), "cache"),
	}
}

func parseV1Options(t *testing.T, args ...string) Options {
	t.Helper()
	opts, err := ParseArgs(args)
	if err != nil {
		t.Fatalf("ParseArgs(%q): %v", args, err)
	}
	return opts
}

func assertV1TrackerTrace(t *testing.T, urls []string, requests []tracker.AnnounceRequest, wantURLs []string, fullLeft int64, metadataPhase bool) {
	t.Helper()
	seen := make(map[string]bool)
	for _, rawURL := range urls {
		seen[rawURL] = true
	}
	for _, rawURL := range wantURLs {
		if !seen[rawURL] {
			t.Errorf("mandatory/configured tracker %q was not routed to the fixture; URLs=%v", rawURL, urls)
		}
	}
	events := make(map[string][]tracker.Event)
	seenMetadataLeft, seenTransferLeft := false, false
	for i, request := range requests {
		if i >= len(urls) {
			t.Fatalf("tracker URL/request trace lengths differ: %d/%d", len(urls), len(requests))
		}
		if request.Uploaded != 0 {
			t.Errorf("announce to %s reported uploaded=%d", urls[i], request.Uploaded)
		}
		if request.Port != 49152 {
			t.Errorf("announce to %s used port %d, want fixed test identity 49152", urls[i], request.Port)
		}
		events[urls[i]] = append(events[urls[i]], request.Event)
		if request.Left == 1 && request.Event == tracker.EventStarted {
			seenMetadataLeft = true
		}
		if request.Left == fullLeft && request.Event == tracker.EventStarted {
			seenTransferLeft = true
		}
	}
	for rawURL, sequence := range events {
		if !hasEventSequence(sequence, tracker.EventStarted, tracker.EventStopped) {
			t.Errorf("tracker %s missing started/stopped trace: %v", rawURL, sequence)
		}
		if !hasEventSequence(sequence, tracker.EventStarted, tracker.EventCompleted, tracker.EventStopped) {
			t.Errorf("tracker %s missing started/completed/stopped trace: %v", rawURL, sequence)
		}
	}
	if !seenTransferLeft || metadataPhase && !seenMetadataLeft {
		t.Errorf("tracker left accounting absent: metadata=%v transfer=%v; requests=%+v", seenMetadataLeft, seenTransferLeft, requests)
	}
}

func hasEventSequence(events []tracker.Event, want ...tracker.Event) bool {
	position := 0
	for _, event := range events {
		if event == want[position] {
			position++
			if position == len(want) {
				return true
			}
		}
	}
	return false
}

func v1Info(t *testing.T, data []byte) ([]byte, torrent.InfoHash) {
	t.Helper()
	piece := sha1.Sum(data)
	value := bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(data))}},
		{Key: []byte("name"), Value: bencode.Value{Type: bencode.Bytes, Bytes: []byte("payload.bin")}},
		{Key: []byte("piece length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(data))}},
		{Key: []byte("pieces"), Value: bencode.Value{Type: bencode.Bytes, Bytes: piece[:]}},
	}}
	encoded, err := bencode.Encode(value)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha1.Sum(encoded)
	var infoHash torrent.InfoHash
	copy(infoHash[:], digest[:])
	return encoded, infoHash
}

func v1TwoPieceInfo(t *testing.T, data []byte) ([]byte, torrent.InfoHash) {
	t.Helper()
	const pieceLength = 16 << 10
	if len(data) != 2*pieceLength {
		t.Fatalf("two-piece fixture length = %d, want %d", len(data), 2*pieceLength)
	}
	pieces := make([]byte, 0, 40)
	for begin := 0; begin < len(data); begin += pieceLength {
		digest := sha1.Sum(data[begin : begin+pieceLength])
		pieces = append(pieces, digest[:]...)
	}
	value := bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(data))}},
		{Key: []byte("name"), Value: bencode.Value{Type: bencode.Bytes, Bytes: []byte("payload.bin")}},
		{Key: []byte("piece length"), Value: bencode.Value{Type: bencode.Integer, Int: pieceLength}},
		{Key: []byte("pieces"), Value: bencode.Value{Type: bencode.Bytes, Bytes: pieces}},
	}}
	encoded, err := bencode.Encode(value)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha1.Sum(encoded)
	var infoHash torrent.InfoHash
	copy(infoHash[:], digest[:])
	return encoded, infoHash
}

func v1MultiInfo(t *testing.T, data []byte) ([]byte, torrent.InfoHash) {
	t.Helper()
	piece := sha1.Sum(data)
	file := func(path string, length int64) bencode.Value {
		return bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
			{Key: []byte("length"), Value: bencode.Value{Type: bencode.Integer, Int: length}},
			{Key: []byte("path"), Value: bencode.Value{Type: bencode.List, List: []bencode.Value{{Type: bencode.Bytes, Bytes: []byte(path)}}}},
		}}
	}
	value := bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("files"), Value: bencode.Value{Type: bencode.List, List: []bencode.Value{file("one.bin", 3), file("two.bin", int64(len(data)-3))}}},
		{Key: []byte("name"), Value: bencode.Value{Type: bencode.Bytes, Bytes: []byte("release")}},
		{Key: []byte("piece length"), Value: bencode.Value{Type: bencode.Integer, Int: int64(len(data))}},
		{Key: []byte("pieces"), Value: bencode.Value{Type: bencode.Bytes, Bytes: piece[:]}},
	}}
	encoded, err := bencode.Encode(value)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha1.Sum(encoded)
	var infoHash torrent.InfoHash
	copy(infoHash[:], digest[:])
	return encoded, infoHash
}

func v1TwoFilePieceInfo(t *testing.T, data []byte) ([]byte, torrent.InfoHash) {
	t.Helper()
	if len(data) != 4 {
		t.Fatalf("two-file fixture length = %d, want 4", len(data))
	}
	piece := sha1.Sum(data)
	file := func(name string) bencode.Value {
		return bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
			{Key: []byte("length"), Value: bencode.Value{Type: bencode.Integer, Int: 2}},
			{Key: []byte("path"), Value: bencode.Value{Type: bencode.List, List: []bencode.Value{{Type: bencode.Bytes, Bytes: []byte(name)}}}},
		}}
	}
	value := bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("files"), Value: bencode.Value{Type: bencode.List, List: []bencode.Value{file("A"), file("B")}}},
		{Key: []byte("name"), Value: bencode.Value{Type: bencode.Bytes, Bytes: []byte("release")}},
		{Key: []byte("piece length"), Value: bencode.Value{Type: bencode.Integer, Int: 4}},
		{Key: []byte("pieces"), Value: bencode.Value{Type: bencode.Bytes, Bytes: piece[:]}},
	}}
	encoded, err := bencode.Encode(value)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha1.Sum(encoded)
	var infoHash torrent.InfoHash
	copy(infoHash[:], digest[:])
	return encoded, infoHash
}

func v1Metainfo(t *testing.T, info []byte, announce string) []byte {
	t.Helper()
	infoValue, err := bencode.Decode(info)
	if err != nil {
		t.Fatal(err)
	}
	entries := []bencode.Entry{{Key: []byte("info"), Value: infoValue}}
	if announce != "" {
		entries = append(entries, bencode.Entry{Key: []byte("announce"), Value: bencode.Value{Type: bencode.Bytes, Bytes: []byte(announce)}})
	}
	root := bencode.Value{Type: bencode.Dictionary, Dict: entries}
	encoded, err := bencode.Encode(root)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func hasV1String(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func writeV1Torrent(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fixture.torrent")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

type v1PeerFixture struct {
	infoHash        torrent.InfoHash
	info            []byte
	data            []byte
	pieceLength     int
	bitfield        []byte
	metadataFirst   bool
	stall           bool
	withhold        bool
	partial         bool
	duplicateHave   bool
	diagnosticChurn int
	requestSeen     chan struct{}
	releasePiece    chan struct{}
	requestOnce     sync.Once
	failAfterFirst  bool
	calls           atomic.Int32
	done            chan error
	mu              sync.Mutex
	outbound        []byte
	requested       [][2]uint32
	requestedPieces []uint32
	serverMu        sync.Mutex
	servers         []net.Conn
}

func newV1Peers(t *testing.T, infoHash torrent.InfoHash, info, data []byte, metadataFirst bool) *v1PeerFixture {
	t.Helper()
	return &v1PeerFixture{infoHash: infoHash, info: append([]byte(nil), info...), data: append([]byte(nil), data...), metadataFirst: metadataFirst, done: make(chan error, 128)}
}

func (f *v1PeerFixture) dial(ctx context.Context, _, _ string) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	client, server := net.Pipe()
	f.serverMu.Lock()
	f.servers = append(f.servers, server)
	f.serverMu.Unlock()
	phase := f.calls.Add(1)
	if f.failAfterFirst && phase > 1 {
		_ = server.Close()
		_ = client.Close()
		return nil, errors.New("fixture refuses reconnect")
	}
	go func() {
		var err error
		if f.metadataFirst && phase == 1 {
			err = f.serveMetadata(server)
		} else {
			err = f.serveTransfer(server)
		}
		f.done <- err
	}()
	return client, nil
}

func (f *v1PeerFixture) serveMetadata(conn net.Conn) error {
	defer conn.Close()
	if _, err := peer.ReadHandshake(conn, (*[20]byte)(&f.infoHash), nil); err != nil {
		return fmt.Errorf("metadata handshake: %w", err)
	}
	if err := peer.WriteHandshake(conn, [20]byte(f.infoHash), [20]byte{0x61}, [8]byte{5: 0x10}); err != nil {
		return err
	}
	if _, err := peer.ReadMessage(conn); err != nil {
		return fmt.Errorf("metadata initial message: %w", err)
	}
	if err := v1WriteFrame(conn, v1ExtensionHandshake(1, int64(len(f.info)))); err != nil {
		return err
	}
	for {
		message, err := v1ReadMessage(conn)
		if err != nil {
			return err
		}
		if message.KeepAlive || message.ID != peer.ExtendedID || len(message.Payload) < 2 {
			continue
		}
		request, err := peer.ParseMetadataControl(message.Payload[1:])
		if err != nil {
			return err
		}
		if request.Type != peer.MetadataRequest {
			continue
		}
		begin := int(request.Piece) * (16 << 10)
		if begin >= len(f.info) {
			return fmt.Errorf("metadata request piece %d outside %d bytes", request.Piece, len(f.info))
		}
		end := min(begin+(16<<10), len(f.info))
		if err := v1WriteFrame(conn, v1MetadataData(1, request.Piece, int64(len(f.info)), f.info[begin:end])); err != nil {
			return err
		}
		if end == len(f.info) {
			return nil
		}
	}
}

func (f *v1PeerFixture) serveTransfer(conn net.Conn) error {
	defer conn.Close()
	if _, err := peer.ReadHandshake(conn, (*[20]byte)(&f.infoHash), nil); err != nil {
		return fmt.Errorf("transfer handshake: %w", err)
	}
	if err := peer.WriteHandshake(conn, [20]byte(f.infoHash), [20]byte{0x62}, [8]byte{7: peer.FastExtensionBit}); err != nil {
		return err
	}
	initial, err := peer.ReadMessage(conn)
	if err != nil {
		return fmt.Errorf("transfer initial message: %w", err)
	}
	if initial.ID != peer.HaveNoneID {
		return fmt.Errorf("initial client message ID %d, want Fast Have None", initial.ID)
	}
	f.mu.Lock()
	f.outbound = append(f.outbound, initial.ID)
	f.mu.Unlock()
	bitfield := f.bitfield
	if len(bitfield) == 0 {
		bitfield = []byte{0x80}
	}
	if err := v1WriteFrame(conn, v1RawMessage(peer.BitfieldID, bitfield)); err != nil {
		return err
	}
	for range f.diagnosticChurn {
		if err := v1WriteFrame(conn, v1RawMessage(peer.ChokeID, nil)); err != nil {
			return err
		}
		if err := v1WriteFrame(conn, v1RawMessage(peer.UnchokeID, nil)); err != nil {
			return err
		}
	}
	if f.duplicateHave {
		have := make([]byte, 4)
		if err := v1WriteFrame(conn, v1RawMessage(peer.HaveID, have)); err != nil {
			return err
		}
		if err := v1WriteFrame(conn, v1RawMessage(peer.HaveID, have)); err != nil {
			return err
		}
	}
	if f.stall {
		_, err := io.Copy(io.Discard, conn)
		return err
	}
	if err := v1WriteFrame(conn, v1RawMessage(peer.UnchokeID, nil)); err != nil {
		return err
	}
	if f.withhold {
		for {
			message, err := peer.ReadMessage(conn)
			if err != nil {
				return err
			}
			if message.ID == peer.RequestID {
				_, err = io.Copy(io.Discard, conn)
				return err
			}
		}
	}
	requested := false
	covered := make([]bool, len(f.data))
	for {
		message, err := peer.ReadMessage(conn)
		if err != nil {
			if requested && (errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF)) {
				return nil
			}
			return err
		}
		if message.KeepAlive {
			continue
		}
		f.mu.Lock()
		f.outbound = append(f.outbound, message.ID)
		f.mu.Unlock()
		switch message.ID {
		case peer.InterestedID:
			continue
		case peer.HaveNoneID:
			continue
		case peer.RequestID:
			if len(message.Payload) != 12 {
				return fmt.Errorf("request payload length %d", len(message.Payload))
			}
			index, begin := binary.BigEndian.Uint32(message.Payload[:4]), binary.BigEndian.Uint32(message.Payload[4:8])
			length := binary.BigEndian.Uint32(message.Payload[8:])
			pieceLength := f.pieceLength
			if pieceLength <= 0 {
				pieceLength = len(f.data)
			}
			absoluteBegin := uint64(index)*uint64(pieceLength) + uint64(begin)
			if length == 0 || absoluteBegin+uint64(length) > uint64(len(f.data)) {
				return fmt.Errorf("request = piece %d begin %d length %d", index, begin, length)
			}
			f.mu.Lock()
			f.requested = append(f.requested, [2]uint32{begin, length})
			f.requestedPieces = append(f.requestedPieces, index)
			f.mu.Unlock()
			if !requested {
				if err := peer.WriteMessage(conn, peer.Message{ID: peer.RequestID, Payload: v1BlockRequest(0, 0, 1)}); err != nil {
					return err
				}
				requested = true
				if f.requestSeen != nil {
					f.requestOnce.Do(func() { close(f.requestSeen) })
				}
				if f.releasePiece != nil {
					<-f.releasePiece
				}
			}
			piece := make([]byte, 8+int(length))
			binary.BigEndian.PutUint32(piece[:4], index)
			binary.BigEndian.PutUint32(piece[4:8], begin)
			copy(piece[8:], f.data[int(absoluteBegin):int(absoluteBegin+uint64(length))])
			if err := v1WriteFrame(conn, v1RawMessage(peer.PieceID, piece)); err != nil {
				return err
			}
			if f.partial {
				_, err := io.Copy(io.Discard, conn)
				return err
			}
			for position := int(absoluteBegin); position < int(absoluteBegin+uint64(length)); position++ {
				covered[position] = true
			}
		case peer.RejectRequestID:
			continue
		case peer.CancelID:
			continue
		default:
			return fmt.Errorf("unexpected client peer message ID %d", message.ID)
		}
	}
}

func (f *v1PeerFixture) wait(t *testing.T) error {
	t.Helper()
	wantCalls := int32(1)
	if f.metadataFirst {
		wantCalls++
	}
	return f.waitCalls(t, wantCalls)
}

func (f *v1PeerFixture) waitCalls(t *testing.T, wantCalls int32) error {
	t.Helper()
	if got := f.calls.Load(); got < wantCalls {
		return fmt.Errorf("TCP fixture dial count = %d, want at least %d", got, wantCalls)
	}
	var joined int32
	for {
		wantJoin := f.calls.Load()
		if joined == wantJoin {
			timer := time.NewTimer(25 * time.Millisecond)
			select {
			case err := <-f.done:
				timer.Stop()
				joined++
				if err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
					return err
				}
				continue
			case <-timer.C:
				if joined == f.calls.Load() {
					return nil
				}
				continue
			}
		}
		select {
		case err := <-f.done:
			joined++
			if err != nil && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
				return err
			}
		case <-time.After(2 * time.Second):
			return fmt.Errorf("peer fixture did not finish (%d of %d joined)", joined, f.calls.Load())
		}
	}
}

func (f *v1PeerFixture) assertNoUpload(t *testing.T) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range f.outbound {
		switch id {
		case peer.UnchokeID, peer.HaveID, peer.BitfieldID, peer.HaveAllID, peer.PieceID:
			t.Errorf("client emitted upload/availability message ID %d", id)
		}
	}
}

func (f *v1PeerFixture) requestedFromStart(t *testing.T) bool {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, request := range f.requested {
		if request[0] == 0 && request[1] == uint32(len(f.data)) {
			return true
		}
	}
	return false
}

func (f *v1PeerFixture) close() {
	f.serverMu.Lock()
	defer f.serverMu.Unlock()
	for _, conn := range f.servers {
		_ = conn.Close()
	}
}

func v1ExtensionHandshake(id byte, total int64) []byte {
	body, _ := bencode.Encode(bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("m"), Value: bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{{Key: []byte("ut_metadata"), Value: bencode.Value{Type: bencode.Integer, Int: int64(id)}}}}},
		{Key: []byte("metadata_size"), Value: bencode.Value{Type: bencode.Integer, Int: total}},
	}})
	return v1ExtensionFrame(0, body)
}

func v1MetadataData(id byte, piece uint32, total int64, block []byte) []byte {
	header, _ := bencode.Encode(bencode.Value{Type: bencode.Dictionary, Dict: []bencode.Entry{
		{Key: []byte("msg_type"), Value: bencode.Value{Type: bencode.Integer, Int: 1}},
		{Key: []byte("piece"), Value: bencode.Value{Type: bencode.Integer, Int: int64(piece)}},
		{Key: []byte("total_size"), Value: bencode.Value{Type: bencode.Integer, Int: total}},
	}})
	return v1ExtensionFrame(id, append(header, block...))
}

func v1ExtensionFrame(id byte, body []byte) []byte {
	payload := append([]byte{id}, body...)
	return v1RawMessage(peer.ExtendedID, payload)
}

func v1BlockRequest(index, begin, length uint32) []byte {
	payload := make([]byte, 12)
	binary.BigEndian.PutUint32(payload[:4], index)
	binary.BigEndian.PutUint32(payload[4:8], begin)
	binary.BigEndian.PutUint32(payload[8:], length)
	return payload
}

func v1RawMessage(id byte, payload []byte) []byte {
	frame := make([]byte, 5+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(1+len(payload)))
	frame[4] = id
	copy(frame[5:], payload)
	return frame
}

func v1WriteFrame(conn net.Conn, frame []byte) error {
	for len(frame) > 0 {
		n, err := conn.Write(frame)
		if err != nil {
			return err
		}
		frame = frame[n:]
	}
	return nil
}

// v1ReadMessage preserves extension payloads so this remote peer fixture can
// interpret requests addressed to its advertised ut_metadata ID (1).
func v1ReadMessage(conn net.Conn) (peer.Message, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(conn, prefix[:]); err != nil {
		return peer.Message{}, err
	}
	length := binary.BigEndian.Uint32(prefix[:])
	if length == 0 {
		return peer.Message{KeepAlive: true}, nil
	}
	if length > 2<<20 {
		return peer.Message{}, fmt.Errorf("fixture peer frame length %d exceeds bound", length)
	}
	wire := make([]byte, int(length))
	if _, err := io.ReadFull(conn, wire); err != nil {
		return peer.Message{}, err
	}
	return peer.Message{ID: wire[0], Payload: wire[1:]}, nil
}
