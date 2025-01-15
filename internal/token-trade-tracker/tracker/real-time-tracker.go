package tokentradetracker

import (
	"context"
	"fmt"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gagliardetto/solana-go/rpc/ws"
	"go.uber.org/zap"

	"github.com/jinbangyi/solanaswap-go/internal/token-trade-tracker/btype"
	tokentradeparser "github.com/jinbangyi/solanaswap-go/pkg/token-trade-parser"
)

// -------------------------------- RealTimeTracker --------------------------------

// SolanaTracker is an implementation of the Tracker interface
type RealTimeTracker struct {
	*BaseTracker

	logSubscribe *ws.LogSubscription
}

// if tokenAddress address is empty then subscribe to all token trades
func NewRealTimeTracker(httpEndpoint []string, wsEndpoint []string, tokenAddress string) (*RealTimeTracker, error) {
	baseTracker, err := NewBaseTracker(httpEndpoint, wsEndpoint, tokenAddress, "RealTimeTracker")
	if err != nil {
		return nil, err
	}

	var logSubscribe *ws.LogSubscription

	// read all trades
	if baseTracker.tokenAddress == "" {
		logSubscribe, err = baseTracker.wsClient.LogsSubscribe(
			ws.LogsSubscribeFilterAll,
			rpc.CommitmentConfirmed,
		)
	} else {
		// read token trades
		logSubscribe, err = baseTracker.wsClient.LogsSubscribeMentions(
			solana.MustPublicKeyFromBase58(baseTracker.tokenAddress),
			rpc.CommitmentConfirmed,
		)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to subscribe logs: %w", err)
	}

	tracker := &RealTimeTracker{
		BaseTracker:  baseTracker,
		logSubscribe: logSubscribe,
	}

	return tracker, nil
}

func (rtt *RealTimeTracker) run() error {
	rtt.LogInfo("start listening for trades: ")

	logResult, err := rtt.logSubscribe.Recv(context.Background())
	if err != nil {
		// TODO if network issue, change the client
		return fmt.Errorf("failed to get logs: %w", err)
	}

	rtt.LogDebug("signature: ", zap.String("signature", logResult.Value.Signature.String()))

	if logResult.Value.Err != nil {
		rtt.LogDebug("transaction error", zap.String("hash", logResult.Value.Signature.String()), zap.Any("error", logResult.Value.Err))
		// ignore the failed transaction
	}

	trade, err := rtt.ParseTrade(context.Background(), logResult)
	if err != nil {
		// TODO full reparse the slot
		rtt.LogError("get trade from logs error", err, zap.Uint64("slot", logResult.Context.Slot))
		// parse the log result failed
	}

	// write to channel
	rtt.tradeChannel <- trade

	return nil
}

func (rtt *RealTimeTracker) ReadTrade(ctx context.Context) (<-chan *btype.Trade, error) {
	go func() {
		err := rtt.FuncTimeoutAndContextDoneWrapper(ctx, time.Duration(5*time.Second), rtt.run)
		if err != nil {
			rtt.LogError("run error", err)
		}

		rtt.Close()
	}()

	return rtt.tradeChannel, nil
}

func (rtt *RealTimeTracker) ParseTrade(ctx context.Context, logResult *ws.LogResult) (*btype.Trade, error) {
	var maxTxVersion uint64 = 0

	// TODO speed up the get tx
	tx, err := rtt.rpcClient.GetTransaction(
		ctx,
		logResult.Value.Signature,
		&rpc.GetTransactionOpts{
			Commitment:                     rpc.CommitmentConfirmed,
			MaxSupportedTransactionVersion: &maxTxVersion,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("error getting tx: %w", err)
	}

	parser, err := tokentradeparser.NewTransactionParser(tx)
	if err != nil {
		return nil, fmt.Errorf("error creating parser: %w", err)
	}

	transactionData, err := parser.ParseTransaction()
	if err != nil {
		return nil, fmt.Errorf("error parsing transaction: %w", err)
	}

	swapData, err := parser.ProcessSwapData(transactionData)
	if err != nil {
		return nil, fmt.Errorf("error processing swap data: %w", err)
	}

	return btype.NewTrade(rtt.String(), swapData, logResult.Context.Slot), nil
}
