package utp

import (
	"bytes"
	"sort"
	"testing"
	"time"
)

// testDatagramLink is intentionally a tiny, test-only link for deterministic
// uTP state tests. It has two numbered endpoints, a manually advanced clock,
// and one scripted behavior per send. Reordering is represented by different
// per-packet delays; duplication and loss are explicit script fields.
type testDatagramLink struct {
	now    time.Duration
	nextID uint64
	script []testLinkStep
	queue  []testLinkDelivery
	inbox  [2][][]byte
}

type testLinkStep struct {
	Drop       bool
	Delay      time.Duration
	Duplicates int
}

type testLinkDelivery struct {
	At      time.Duration
	Order   uint64
	To      int
	Payload []byte
}

func newTestDatagramLink(steps ...testLinkStep) *testDatagramLink {
	for _, step := range steps {
		if step.Delay < 0 || step.Duplicates < 0 {
			panic("negative test link behavior")
		}
	}
	return &testDatagramLink{script: append([]testLinkStep(nil), steps...)}
}

func (l *testDatagramLink) Send(from int, payload []byte) {
	if from != 0 && from != 1 {
		panic("test link endpoint must be 0 or 1")
	}
	step := testLinkStep{}
	if len(l.script) != 0 {
		step = l.script[0]
		l.script = l.script[1:]
	}
	if step.Drop {
		return
	}
	for copyIndex := 0; copyIndex <= step.Duplicates; copyIndex++ {
		l.nextID++
		l.queue = append(l.queue, testLinkDelivery{
			At:      l.now + step.Delay,
			Order:   l.nextID,
			To:      1 - from,
			Payload: append([]byte(nil), payload...),
		})
	}
}

func (l *testDatagramLink) Advance(delta time.Duration) {
	if delta < 0 {
		panic("negative test link clock advance")
	}
	l.now += delta
	sort.SliceStable(l.queue, func(i, j int) bool {
		if l.queue[i].At != l.queue[j].At {
			return l.queue[i].At < l.queue[j].At
		}
		return l.queue[i].Order < l.queue[j].Order
	})
	ready := 0
	for ready < len(l.queue) && l.queue[ready].At <= l.now {
		delivery := l.queue[ready]
		l.inbox[delivery.To] = append(l.inbox[delivery.To], delivery.Payload)
		ready++
	}
	l.queue = append([]testLinkDelivery(nil), l.queue[ready:]...)
}

func (l *testDatagramLink) Recv(side int) ([]byte, bool) {
	if side != 0 && side != 1 {
		panic("test link endpoint must be 0 or 1")
	}
	if len(l.inbox[side]) == 0 {
		return nil, false
	}
	payload := l.inbox[side][0]
	l.inbox[side] = l.inbox[side][1:]
	return payload, true
}

func TestDatagramLinkScriptsLossDelayReorderingDuplication(t *testing.T) {
	link := newTestDatagramLink(
		testLinkStep{Delay: 10 * time.Millisecond},
		testLinkStep{},
		testLinkStep{Duplicates: 1},
		testLinkStep{Drop: true},
	)
	link.Send(0, []byte("first"))
	link.Send(0, []byte("second"))
	link.Send(0, []byte("third"))
	link.Send(0, []byte("lost"))

	link.Advance(0)
	got, ok := link.Recv(1)
	if !ok || !bytes.Equal(got, []byte("second")) {
		t.Fatalf("first ready datagram = %q, %v", got, ok)
	}
	got, ok = link.Recv(1)
	if !ok || !bytes.Equal(got, []byte("third")) {
		t.Fatalf("second ready datagram = %q, %v", got, ok)
	}
	got, ok = link.Recv(1)
	if !ok || !bytes.Equal(got, []byte("third")) {
		t.Fatalf("duplicate datagram = %q, %v", got, ok)
	}
	if _, ok := link.Recv(1); ok {
		t.Fatal("unexpected datagram before delayed packet")
	}
	link.Advance(10 * time.Millisecond)
	got, ok = link.Recv(1)
	if !ok || !bytes.Equal(got, []byte("first")) {
		t.Fatalf("delayed datagram = %q, %v", got, ok)
	}
	if _, ok := link.Recv(1); ok {
		t.Fatal("dropped datagram was delivered")
	}
}
