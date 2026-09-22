package utp

import (
	"math"
	"testing"
	"time"

	"github.com/gus-ceraso/Leech/internal/limits"
)

func TestBoundedGainAtSupportedExtremes(t *testing.T) {
	maxPacket := int(limits.DatagramBytes) - HeaderBytes
	maxReported := time.Duration(^uint32(0)) * time.Microsecond
	maxFlight := ^uint32(0)

	positive := boundedGain(maxPacket, time.Second, time.Second, maxFlight, 1)
	if positive <= 0 || positive > int64(limits.UTPBufferBytes-1) {
		t.Fatalf("positive boundary gain = %d", positive)
	}
	negative := boundedGain(maxPacket, -maxReported, time.Nanosecond, maxFlight, uint32(limits.UTPBufferBytes))
	if negative >= 0 || negative < -int64(limits.UTPBufferBytes) {
		t.Fatalf("negative boundary gain = %d", negative)
	}

	controller, err := NewCongestionController(CongestionConfig{
		InitialWindow: uint32(limits.UTPBufferBytes),
		InitialPacket: maxPacket,
		MinPacket:     minimumPacketSize,
		MaxPacket:     maxPacket,
		TargetDelay:   time.Nanosecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	controller.ObserveDelay(now, 0, maxFlight)
	controller.ObserveDelay(now.Add(time.Second), maxReported, maxFlight)
	if got := controller.MaxWindow(); got > uint32(limits.UTPBufferBytes) {
		t.Fatalf("window wrapped or exceeded bound: %d", got)
	}
	if got := controller.PacketSize(); got < minimumPacketSize || got > maxPacket {
		t.Fatalf("packet size escaped bound: %d", got)
	}
}

func TestBoundedGainPreservesDirectionAndRecovery(t *testing.T) {
	controller, err := NewCongestionController(CongestionConfig{
		InitialWindow: 4_096,
		InitialPacket: 1_024,
		MinPacket:     minimumPacketSize,
		MaxPacket:     1_024,
		TargetDelay:   100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(110, 0)
	controller.ObserveDelay(now, 0, 4_096)
	lowDelayWindow := controller.MaxWindow()
	if lowDelayWindow <= 4_096 {
		t.Fatalf("below-target delay did not grow window: %d", lowDelayWindow)
	}

	controller.ObserveDelay(now.Add(time.Second), 300*time.Millisecond, 4_096)
	highDelayWindow := controller.MaxWindow()
	if highDelayWindow >= lowDelayWindow {
		t.Fatalf("above-target delay did not shrink window: low=%d high=%d", lowDelayWindow, highDelayWindow)
	}
	controller.OnLoss()
	if controller.MaxWindow() >= highDelayWindow {
		t.Fatalf("loss did not reduce window: before=%d after=%d", highDelayWindow, controller.MaxWindow())
	}
	controller.OnTimeout()
	if controller.MaxWindow() != uint32(minimumPacketSize) || controller.PacketSize() != minimumPacketSize {
		t.Fatalf("timeout recovery seed: window=%d packet=%d", controller.MaxWindow(), controller.PacketSize())
	}
	controller.ObserveDelay(now.Add(2*time.Second), 0, uint32(minimumPacketSize))
	if controller.MaxWindow() <= uint32(minimumPacketSize) {
		t.Fatalf("below-target delay did not recover timeout window: %d", controller.MaxWindow())
	}
}

func TestMulDivCapNeverWraps(t *testing.T) {
	values := [][4]uint64{
		{math.MaxUint64, math.MaxUint64, 1, 7},
		{math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64},
		{1 << 63, 1 << 63, 3, 4_194_304},
	}
	for _, value := range values {
		got := mulDivCap(value[0], value[1], value[2], value[3])
		if got > value[3] {
			t.Fatalf("mulDivCap(%v) = %d above cap", value, got)
		}
	}
}
