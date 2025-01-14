package tokentradetracker

import (
	solanaswapgo "github.com/franco-bianco/solanaswap-go/solanaswap-go"

	bkafka "github.com/jinbangyi/solanaswap-go/pkg/kafka"
)

type SwapInfo struct {
	Signers    []string `json:"signers"`
	Signatures []string `json:"signatures"`
	AMMs       []string `json:"AMMs"`
	Timestamp  int64    `json:"timestamp"`

	TokenInMint     string `json:"tokenInMint"`
	TokenInAmount   uint64 `json:"tokenInAmount"`
	TokenInDecimals uint8  `json:"tokenInDecimals"`

	TokenOutMint     string `json:"tokenOutMint"`
	TokenOutAmount   uint64 `json:"tokenOutAmount"`
	TokenOutDecimals uint8  `json:"tokenOutDecimals"`
}

// Trade represents a trade data structure
type Trade struct {
	SwapInfo *SwapInfo `json:"swapInfo"`
	// tracker's name
	Tracker string `json:"tracker"`
	Slot   uint64 `json:"slot"`
}

func NewTrade(trackerName string, swapInfo *solanaswapgo.SwapInfo, slot uint64) *Trade {
	signers := make([]string, len(swapInfo.Signers))
	for i, signer := range swapInfo.Signers {
		signers[i] = signer.String()
	}

	signatures := make([]string, len(swapInfo.Signatures))
	for i, signature := range swapInfo.Signatures {
		signatures[i] = signature.String()
	}

	return &Trade{
		SwapInfo: &SwapInfo{
			Signers:          signers,
			Signatures:       signatures,
			AMMs:             swapInfo.AMMs,
			Timestamp:        swapInfo.Timestamp.UnixMilli(),
			TokenInMint:      swapInfo.TokenInMint.String(),
			TokenInAmount:    swapInfo.TokenInAmount,
			TokenInDecimals:  swapInfo.TokenInDecimals,
			TokenOutMint:     swapInfo.TokenOutMint.String(),
			TokenOutAmount:   swapInfo.TokenOutAmount,
			TokenOutDecimals: swapInfo.TokenOutDecimals,
		},
		Tracker: trackerName,
		Slot:    slot,
	}
}

const (
	SolanaTradeTopic = "solana-trade"
)

// CollectionContractEvent 合约和 collection 关系变更事件
type TradeEvent struct {
	bkafka.EventBase
	Trade *Trade `json:"trade"`
}

func (te TradeEvent) GetConsumers() []bkafka.ConsumerTopicPartition {
	return []bkafka.ConsumerTopicPartition{
		{Topic: SolanaTradeTopic, PartitionKey: ""},
	}
}
