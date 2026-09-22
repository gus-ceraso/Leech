package limits

import (
	"math"
	"testing"
)

func TestCheckedArithmetic(t *testing.T) {
	for _, tc := range []struct {
		a, b int64
		want int64
		ok   bool
	}{
		{1, 2, 3, true},
		{math.MaxInt64, 1, 0, false},
		{math.MinInt64, -1, 0, false},
	} {
		got, ok := Add(tc.a, tc.b)
		if got != tc.want || ok != tc.ok {
			t.Fatalf("Add(%d, %d) = (%d, %t)", tc.a, tc.b, got, ok)
		}
	}
	if _, ok := Mul(math.MaxInt64, 2); ok {
		t.Fatal("overflowing multiplication succeeded")
	}
	if _, ok := Mul(math.MinInt64, -1); ok {
		t.Fatal("minimum integer times -1 succeeded")
	}
	if got, ok := Mul(2, 3); !ok || got != 6 {
		t.Fatalf("Mul(2, 3) = (%d, %t)", got, ok)
	}
}
