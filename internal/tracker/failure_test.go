package tracker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func TestHTTPParserFailureCauses(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		code       HTTPErrorCode
		cause      FailureCause
		permanent  bool
	}{
		{"bencode", "d8:intervali60e5:peers6:x", HTTPErrorMalformed, CauseInvalidBencode, false},
		{"field", "d8:intervali60e5:peersi1ee", HTTPErrorMalformed, CauseInvalidResponseField, false},
		{"dictionary peer", "d8:intervali60e5:peersldeee", HTTPErrorMalformed, CauseInvalidDictionaryPeer, false},
		{"v4 stride", "d8:intervali60e5:peers1:xe", HTTPErrorMalformed, CauseIncompleteCompactPeer, false},
		{"v6 stride", "d8:intervali60e5:peers0:6:peers61:xe", HTTPErrorMalformed, CauseIncompleteCompactPeer, false},
		{"v4 address", "d8:intervali60e5:peers6:\x00\x00\x00\x00\x00\x01e", HTTPErrorMalformed, CauseInvalidCompactEndpoint, false},
		{"v4 port after valid peer", "d8:intervali60e5:peers12:\xc0\x00\x02\x01\x1a\xe1\xc0\x00\x02\x02\x00\x00e", HTTPErrorMalformed, CauseInvalidCompactEndpoint, false},
		{"v6 address", "d8:intervali60e5:peers0:6:peers618:" + string(make([]byte, 17)) + "\x01e", HTTPErrorMalformed, CauseInvalidCompactEndpoint, false},
		{"interval", "d8:intervali0e5:peers0:e", HTTPErrorInterval, CauseInvalidInterval, true},
		{"retry delay", "d14:failure reason6:secret8:retry ini0ee", HTTPErrorRetryDelay, CauseInvalidRetryDelay, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := parseHTTPAnnounceResponse([]byte(tc.body))
			var failure *HTTPError
			if !errors.As(err, &failure) || failure.Code != tc.code || CauseOf(fmt.Errorf("wrapped: %w", err)) != tc.cause {
				t.Fatalf("failure = %+v, cause = %v; want %s/%s", failure, CauseOf(err), tc.code, tc.cause)
			}
			if len(result.Peers) != 0 || isPermanent(err, 0) != tc.permanent {
				t.Fatalf("rejection/retry policy changed: peers=%v permanent=%t", result.Peers, isPermanent(err, 0))
			}
		})
	}
}

func TestUDPParserFailureCauses(t *testing.T) {
	// BEP 15 announce: action=1, transaction=42, interval=60, no peers.
	header := []byte{0, 0, 0, 1, 0, 0, 0, 42, 0, 0, 0, 60, 0, 0, 0, 0, 0, 0, 0, 0}
	for _, v4 := range []bool{true, false} {
		stride := 18
		if v4 {
			stride = 6
		}
		invalid := make([]byte, stride)
		invalid[stride-1] = 1 // Unspecified address, nonzero port.
		valid := bytes.Clone(invalid)
		valid[0], valid[1], valid[2], valid[3] = 32, 1, 13, 184
		badPort := bytes.Clone(valid)
		badPort[stride-1] = 0
		badInterval := bytes.Clone(header)
		badInterval[11] = 0
		for _, tc := range []struct {
			name      string
			packet    []byte
			cause     FailureCause
			permanent bool
		}{
			{"header", header[:19], CauseIncompleteResponse, false},
			{"stride", append(bytes.Clone(header), 1), CauseIncompleteCompactPeer, false},
			{"address", append(bytes.Clone(header), invalid...), CauseInvalidCompactEndpoint, false},
			{"port after valid peer", append(append(bytes.Clone(header), valid...), badPort...), CauseInvalidCompactEndpoint, false},
			{"interval", badInterval, CauseInvalidInterval, true},
		} {
			t.Run(fmt.Sprintf("v4=%t/%s", v4, tc.name), func(t *testing.T) {
				result, err := parseAnnounceResponse(tc.packet, 42, v4)
				var failure *Error
				if !errors.As(err, &failure) || failure.Code != ErrorMalformed || CauseOf(err) != tc.cause {
					t.Fatalf("failure = %+v, cause = %v; want malformed/%s", failure, CauseOf(err), tc.cause)
				}
				if len(result.Peers) != 0 || isPermanent(err, 0) != tc.permanent {
					t.Fatalf("rejection/retry policy changed: peers=%v permanent=%t", result.Peers, isPermanent(err, 0))
				}
			})
		}
	}
}

func TestTrackerContextFailureCauses(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprintf("deadline=%t", deadline), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			want, cause, udpCode, httpCode := error(context.Canceled), CauseCanceled, ErrorCanceled, HTTPErrorCanceled
			if deadline {
				cancel()
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				want, cause, udpCode, httpCode = context.DeadlineExceeded, CauseDeadline, ErrorTimeout, HTTPErrorTimeout
			}
			defer cancel()
			cancel()
			client := NewUDPClient(Config{Resolver: fixtureResolver{ips: []net.IPAddr{{IP: net.IPv4(127, 0, 0, 1)}}}})
			defer client.Close()
			result, err := client.Announce(ctx, "udp://fixture.test:1/secret", testRequest())
			var failure *Error
			if !errors.As(err, &failure) || failure.Code != udpCode || CauseOf(err) != cause || !errors.Is(err, want) || result.Transmitted {
				t.Fatalf("UDP result=%+v error=%+v cause=%s", result, failure, CauseOf(err))
			}
			// The same boundary retains transmission when a waiting exchange ends.
			failure = udpContextError("announce", true, want)
			if failure.Code != udpCode || !failure.Transmitted || !errors.Is(failure, want) || CauseOf(failure) != cause {
				t.Fatalf("transmitted UDP error=%+v", failure)
			}
			httpFailure := httpTransportError(errors.New("untrusted transport error"), true, ctx)
			if httpFailure.Code != httpCode || !httpFailure.Transmitted || CauseOf(httpFailure) != cause {
				t.Fatalf("HTTP error=%+v cause=%s", httpFailure, CauseOf(httpFailure))
			}
			if deadline && !errors.Is(httpFailure, ErrHTTPTimeout) {
				t.Fatal("HTTP timeout sentinel was lost")
			}
		})
	}
}

func TestTrackerFailureCauseUsesIdentityNotText(t *testing.T) {
	for _, tc := range []struct {
		err   error
		cause FailureCause
	}{
		{fmt.Errorf("secret: %w", context.DeadlineExceeded), CauseDeadline},
		{&HTTPError{Code: HTTPErrorResponse, Err: io.ErrUnexpectedEOF}, CauseIncompleteResponse},
		{&Error{Code: ErrorRead, Err: &net.DNSError{Err: "secret", IsTimeout: true}}, CauseTimeout},
		{ErrTimeout, CauseTimeout},
		{errors.New("invalid compact peer endpoint"), CauseUnknown},
		{errors.New("context deadline exceeded"), CauseUnknown},
		{errors.New("secret\nhttps://user:password@example.test/path?token=secret"), CauseUnknown},
	} {
		if got := CauseOf(tc.err); got != tc.cause {
			t.Errorf("cause = %s, want %s", got, tc.cause)
		}
	}
	if got := FailureCause(255).String(); got != "unknown" {
		t.Fatalf("unknown cause label = %q", got)
	}
}
