package peer

import "net/netip"

// Endpoint is a resolved peer identity. TCP and uTP share the same endpoint.
// Candidates validates the address and port before constructing this value.
type Endpoint struct {
	Addr netip.Addr
	Port uint16
}
