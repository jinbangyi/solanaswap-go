package tokentradetracker

func WatchLatestTrade(tokenAddress string) {
	// tokenAddress := "HeLp6NuQkmYB4pYWo2zYs22mESHXPQYzXbB8n4V98jwC"
	httpEndpoint := []string{"https://mainnet.helius-rpc.com/?api-key=6bfb79c3-fd6e-4a61-bae2-a097f611fa3a"}
	wsEndpoint := []string{"wss://mainnet.helius-rpc.com/?api-key=6bfb79c3-fd6e-4a61-bae2-a097f611fa3a"}

	job, err := NewGetLatestTradeJob(tokenAddress, httpEndpoint, wsEndpoint)
	if err != nil {
		panic(err)
	}

	err = job.Run()
	if err != nil {
		panic(err)
	}
}
