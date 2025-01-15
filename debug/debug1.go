package main

import (
	tokentradetracker "github.com/jinbangyi/solanaswap-go/internal/token-trade-tracker"
)

// func main() {
// 	TokenTradeTracker, err := tokentradetracker.NewRealTimeTracker(
// 		// []string{"HeLp6NuQkmYB4pYWo2zYs22mESHXPQYzXbB8n4V98jwC"},
// 		// "wss://lb.drpc.org/ogws?network=solana&dkey=ApWQL9OFaUcAvu4fN3gvbNFnMK4A0ZQR75-2QkTKRtpJ",
// 		// "https://lb.drpc.org/ogrpc?network=solana&dkey=ApWQL9OFaUcAvu4fN3gvbNFnMK4A0ZQR75-2QkTKRtpJ",
// 		// "https://solana-mainnet.g.alchemy.com/v2/LLCv8Z62D7dXYdQNuWrTZIKcnitjb1Er",
// 		[]string{"https://mainnet.helius-rpc.com/?api-key=6bfb79c3-fd6e-4a61-bae2-a097f611fa3a"},
// 		[]string{"wss://mainnet.helius-rpc.com/?api-key=6bfb79c3-fd6e-4a61-bae2-a097f611fa3a"},
// 		"HeLp6NuQkmYB4pYWo2zYs22mESHXPQYzXbB8n4V98jwC",
// 	)
// 	if err != nil {
// 		panic(err)
// 	}

// 	tradeChannel, err := TokenTradeTracker.ReadTrade()
// 	if err != nil {
// 		panic(err)
// 	}

// 	for trade := range tradeChannel {
// 		// do something with the trade
// 		log.Printf("Trade: %+v", trade)
// 	}
// }

// func main() {
// 	TokenTradeTracker, err := tokentradetracker.NewHistoryTracker(
// 		// []string{"HeLp6NuQkmYB4pYWo2zYs22mESHXPQYzXbB8n4V98jwC"},
// 		// "wss://lb.drpc.org/ogws?network=solana&dkey=ApWQL9OFaUcAvu4fN3gvbNFnMK4A0ZQR75-2QkTKRtpJ",
// 		// "https://lb.drpc.org/ogrpc?network=solana&dkey=ApWQL9OFaUcAvu4fN3gvbNFnMK4A0ZQR75-2QkTKRtpJ",
// 		// "https://solana-mainnet.g.alchemy.com/v2/LLCv8Z62D7dXYdQNuWrTZIKcnitjb1Er",
// 		[]string{"https://mainnet.helius-rpc.com/?api-key=6bfb79c3-fd6e-4a61-bae2-a097f611fa3a"},
// 		[]string{"wss://mainnet.helius-rpc.com/?api-key=6bfb79c3-fd6e-4a61-bae2-a097f611fa3a"},
// 		313632000,
// 		313633000,
// 	)
// 	if err != nil {
// 		panic(err)
// 	}

// 	tradeChannel, err := TokenTradeTracker.ReadTrade()
// 	if err != nil {
// 		panic(err)
// 	}

// 	for trade := range tradeChannel {
// 		// do something with the trade
// 		log.Printf("Trade: %+v", trade)
// 	}
// }

func main() {
	tokenAddress := "HeLp6NuQkmYB4pYWo2zYs22mESHXPQYzXbB8n4V98jwC"
	tokentradetracker.WatchLatestTrade(tokenAddress)
}
