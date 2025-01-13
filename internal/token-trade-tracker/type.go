package tokentradetracker

import (
	solanaswapgo "github.com/franco-bianco/solanaswap-go/solanaswap-go"

	tokentradeparser "github.com/jinbangyi/solanaswap-go/internal/token-trade-parser"
)

// Trade represents a trade data structure
type Trade struct {
	SwapInfo *solanaswapgo.SwapInfo
	// tracker's name
	Tracker string
}

type Trade2 struct {
	SwapInfo *tokentradeparser.SwapInfo
	// tracker's name
	Tracker string
}
