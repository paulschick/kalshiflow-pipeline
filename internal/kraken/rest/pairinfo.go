package rest

import "encoding/json"

// PairInfo is the subset of /0/public/AssetPairs[pair] kalshiflow consumes.
// All numeric wire fields stay as json.Number — see HOL-48 risk table; float64
// is never on the path for prices, sizes, or tick sizes.
type PairInfo struct {
	Pair         string      `json:"-"`
	Altname      string      `json:"altname"`
	Wsname       string      `json:"wsname"`
	PairDecimals int         `json:"pair_decimals"`
	LotDecimals  int         `json:"lot_decimals"`
	TickSize     json.Number `json:"tick_size"`
	Status       string      `json:"status"`
}
