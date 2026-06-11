package orderbook

// Side discriminates bid vs ask. Disjoint from internal/orderbook.Side
// (which is SideYes/SideNo for Kalshi binary markets).
type Side int

// Side enum values: zero value SideUnknown is invalid and panics in toKalshiSide.
const (
	SideUnknown Side = iota
	SideBid
	SideAsk
)

// String returns the wire-format side name.
func (s Side) String() string {
	switch s {
	case SideBid:
		return "bid"
	case SideAsk:
		return "ask"
	default:
		return "unknown"
	}
}
