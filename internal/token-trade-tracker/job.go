package tokentradetracker

import (
	"context"
	"fmt"

	"github.com/jinbangyi/solanaswap-go/internal/token-trade-tracker/handler"
	"github.com/jinbangyi/solanaswap-go/internal/token-trade-tracker/tracker"
	"github.com/jinbangyi/solanaswap-go/pkg/log"
	"go.uber.org/zap"
)

type GetLatestTradeJob struct {
	RealTimeTracker *tracker.RealTimeTracker
	KafkaHandler    *handler.KafkaHandler
}

func NewGetLatestTradeJob(tokenAddress string, httpEndpoint []string, wsEndpoint []string) (*GetLatestTradeJob, error) {
	tracker, err := tracker.NewRealTimeTracker(
		httpEndpoint,
		wsEndpoint,
		tokenAddress,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create tracker: %w", err)
	}

	return &GetLatestTradeJob{
		RealTimeTracker: tracker,
		KafkaHandler:    handler.NewKafkaHandler(fmt.Sprintf("GetLatestTradeJob:kafkaHandler:%s", tokenAddress)),
	}, nil
}

func (job *GetLatestTradeJob) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tradeChannel, err := job.RealTimeTracker.ReadTrade(ctx)
	if err != nil {
		return fmt.Errorf("failed to read trade: %w", err)
	}

	// job.KafkaHandler.WriteTrade()

	for trade := range tradeChannel {
		log.Debug("trade", zap.Uint64("slot", trade.Slot), zap.String("hash", trade.SwapInfo.Signatures[0]))
		// TODO 
		err := job.KafkaHandler.WriteTrade(ctx, trade)
		if err != nil {
			return fmt.Errorf("failed to write trade: %w", err)
		}
	}

	return nil
}
