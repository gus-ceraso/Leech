package utp

import (
	"errors"
	"math"
	"math/bits"
	"time"

	"github.com/gus-ceraso/Leech/internal/limits"
)

var ErrInvalidCongestionConfig = errors.New("utp: invalid congestion configuration")

// uTP's delay controller is deliberately kept independent of the socket. The
// sender owns one controller and feeds it measured timestamp-difference
// feedback and newly acknowledged bytes from the peer. This also makes the controller useful in
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
// so only its change from the sliding minimum is meaningful. ackedBytes is
// newly acknowledged payload, or zero to update only the delay history.
func (c *CongestionController) ObserveDelay(now time.Time, reported time.Duration, ackedBytes uint32) {
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
	if ackedBytes == 0 || c.maxWindow == 0 {
		return
	}
	ourDelay := reported - c.baseDelay
	offTarget := c.target - ourDelay

	// The BEP expression scales a gain measured in packets per RTT by the
	// acknowledged share of the window. Unrelated peer traffic earns no
	// credit, and duplicate ACKs cannot apply the same credit twice.
	gain := boundedGain(c.packet, offTarget, c.target, ackedBytes, c.maxWindow)
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

// boundedGain evaluates packet * offTarget / target * outstanding / window
// without allowing an intermediate product to wrap. Only the final change
// that can affect the bounded window is needed: positive gain is capped at
// the remaining supported window, and negative gain is capped at the current
// window. The two staged divisions preserve the BEP expression's truncation
// order while mulDivCap handles products wider than uint64 safely.
func boundedGain(packet int, offTarget, target time.Duration, outstanding, maxWindow uint32) int64 {
	if packet <= 0 || offTarget == 0 || target <= 0 || outstanding == 0 || maxWindow == 0 {
		return 0
	}
	negative := offTarget < 0
	absOffTarget := uint64(offTarget)
	if negative {
		// Avoid negating the minimum signed integer in a general helper. The
		// current caller's timestamp range is smaller, but this keeps the
		// arithmetic invariant explicit.
		absOffTarget = uint64(-(offTarget + 1)) + 1
	}
	windowLimit := uint64(limits.UTPBufferBytes)
	capGain := uint64(maxWindow)
	if !negative {
		if uint64(maxWindow) >= windowLimit {
			return 0
		}
		capGain = windowLimit - uint64(maxWindow)
	}
	if capGain == 0 {
		return 0
	}

	// If the first quotient exceeds this threshold, the final quotient is
	// already at least capGain. The multiplication is bounded by 4 MiB².
	firstCap := capGain*uint64(maxWindow)/uint64(outstanding) + 1
	first := mulDivCap(uint64(packet), absOffTarget, uint64(target), firstCap)
	second := mulDivCap(first, uint64(outstanding), uint64(maxWindow), capGain)
	if second == 0 {
		second = 1
	}
	if negative {
		return -int64(second)
	}
	return int64(second)
}

// mulDivCap returns min(cap, floor(a*b/denominator)). It uses a 128-bit
// product comparison before division, so no product can wrap and the bounded
// result remains exact even when a*b does not fit in uint64.
func mulDivCap(a, b, denominator, cap uint64) uint64 {
	if a == 0 || b == 0 || denominator == 0 || cap == 0 {
		return 0
	}
	productHigh, productLow := bits.Mul64(a, b)
	capHigh, capLow := bits.Mul64(cap, denominator)
	if productHigh > capHigh || (productHigh == capHigh && productLow >= capLow) {
		return cap
	}
	if productHigh >= denominator {
		// The quotient would exceed uint64, and therefore also exceeds cap.
		return cap
	}
	quotient, _ := bits.Div64(productHigh, productLow, denominator)
	if quotient > cap {
		return cap
	}
	return quotient
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
		c.rto = saturatingDurationAdd(c.rtt, saturatingDurationMultiply(c.rttVar, 4))
		if c.rto < minimumRTO {
			c.rto = minimumRTO
		}
		return
	}
	// Keep the estimator in separate fields without exposing them as tuning
	// knobs. These values are initialized lazily to avoid a second constructor
	// state for the first sample.
	delta := absoluteDurationDifference(c.rtt, sample)
	c.rttVar = saturatingDurationAdd(c.rttVar, (delta-c.rttVar)/4)
	c.rtt = saturatingDurationAdd(c.rtt, (sample-c.rtt)/8)
	c.rto = saturatingDurationAdd(c.rtt, saturatingDurationMultiply(c.rttVar, 4))
	if c.rto < minimumRTO {
		c.rto = minimumRTO
	}
}

func saturatingDurationMultiply(value time.Duration, multiplier int64) time.Duration {
	if value <= 0 || multiplier <= 0 {
		return 0
	}
	if value > time.Duration(math.MaxInt64)/time.Duration(multiplier) {
		return time.Duration(math.MaxInt64)
	}
	return value * time.Duration(multiplier)
}

func saturatingDurationAdd(a, b time.Duration) time.Duration {
	if b > 0 && a > time.Duration(math.MaxInt64)-b {
		return time.Duration(math.MaxInt64)
	}
	if b < 0 && a < time.Duration(math.MinInt64)-b {
		return time.Duration(math.MinInt64)
	}
	return a + b
}

func absoluteDurationDifference(a, b time.Duration) time.Duration {
	if a >= b {
		if b < 0 && a > time.Duration(math.MaxInt64)+b {
			return time.Duration(math.MaxInt64)
		}
		return a - b
	}
	if a < 0 && b > time.Duration(math.MaxInt64)+a {
		return time.Duration(math.MaxInt64)
	}
	return b - a
}

func (c *CongestionController) hasRTT() bool { return c.rttSet }
