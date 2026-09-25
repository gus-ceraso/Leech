package utp

import (
	"context"
	"fmt"
	"net"
)

// DialContext opens one connected UDP socket and completes an outgoing uTP
// handshake. The returned value implements net.Conn and owns the socket.
// network accepts utp, utp4, utp6, udp, udp4, and udp6; utp names are aliases
// that make transport selection explicit at the caller boundary.
func DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return (&Dialer{}).DialContext(ctx, network, address)
}

// Dial is the convenience form of DialContext using a background context.
func Dial(network, address string) (net.Conn, error) {
	return DialContext(context.Background(), network, address)
}

// Dialer is the uTP dialer. It intentionally has no listener or server
// configuration: every call owns exactly one connected UDP socket.
type Dialer struct{}

func (d *Dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	udpNetwork, err := udpNetworkName(network)
	if err != nil {
		return nil, err
	}
	dialer := net.Dialer{}
	raw, err := dialer.DialContext(ctx, udpNetwork, address)
	if err != nil {
		return nil, err
	}
	udp, ok := raw.(*net.UDPConn)
	if !ok {
		_ = raw.Close()
		return nil, fmt.Errorf("utp: UDP dial returned %T", raw)
	}
	// The caller's context bounds setup, not the lifetime of a successfully
	// returned net.Conn. DialContext closes and joins this worker if setup is
	// canceled or fails below.
	conn, err := NewConn(context.Background(), udp)
	if err != nil {
		_ = udp.Close()
		return nil, err
	}
	if err := conn.waitEstablished(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func udpNetworkName(network string) (string, error) {
	switch network {
	case "", "utp", "udp":
		return "udp", nil
	case "utp4", "udp4":
		return "udp4", nil
	case "utp6", "udp6":
		return "udp6", nil
	default:
		return "", fmt.Errorf("utp: unsupported network %q", network)
	}
}
