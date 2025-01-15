package tracker

import (
	"context"
	"fmt"
	"time"

	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gagliardetto/solana-go/rpc/ws"
	"github.com/jinbangyi/solanaswap-go/pkg/log"
	tokentradeparser "github.com/jinbangyi/solanaswap-go/pkg/token-trade-parser"
	"go.uber.org/zap"

	"github.com/jinbangyi/solanaswap-go/internal/token-trade-tracker/btype"
)

// Tracker interface defines methods for reading trade data from a Solana node
// type Tracker interface {
// 	// get history trades
// 	ReadTokenHistoricalTrades(tokenAddress solana.PublicKey) ([]Trade, error)
// 	ReadHistoricalTrades() ([]Trade, error)

// 	// get real-time trades
// 	ReadTokenRealTimeTrades(tokenAddress solana.PublicKey) (<-chan *Trade, error)
// 	ReadRealTimeTrades() (<-chan *Trade, error)

// 	// using the special strategy to prevent the unstable of the node
// 	GetClient() *ws.Client
// 	GetWSClient() *rpc.Client

// 	// inbuilt method
// 	// TODO define strategy interface
// 	ChangeClientStrategy() error
// }

type TrackerI interface {
	// read trade from the tracker
	ReadTrade(ctx context.Context) (<-chan *btype.Trade, error)
	// parse trade from the tracker
	ParseTrade(originTrade any) (*btype.Trade, error)
	GetHttpClient() *rpc.Client
	GetWsClient() *ws.Client
	// tracker instance id
	String() string
	Close() error
	// LogError(message string, err error)
}

// -------------------------------- BaseTracker --------------------------------

type BaseTracker struct {
	httpEndpoint []string
	wsEndpoint   []string

	wsClient  *ws.Client
	rpcClient *rpc.Client

	tokenAddress string
	trackerName  string

	tradeChannel chan *btype.Trade

	// trade channel buffer size
	channelBufferSize int
	// current sleep time when retrying
	retrySleepTime     int
	retryInitSleepTime int
	maxRetry           int
	currentRetry       int
}

func (bt *BaseTracker) FuncTimeoutAndContextDoneWrapper(
	ctx context.Context,
	tickerLoop time.Duration,
	fn func() error,
) error {
	ticker := time.NewTicker(tickerLoop)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			bt.LogInfo("context done")
			return ctx.Err()
		case <-ticker.C:
			bt.LogWarn("Tracker progress timeout")
		default:
			if err := fn(); err != nil {
				bt.LogError("function execution error", err)

				// sleep and retry
				ticker.Reset(tickerLoop + time.Duration(bt.retrySleepTime)*time.Second)
				bt.RetrySleep(err)
			}

			bt.RetrySleep(nil)
			ticker.Reset(tickerLoop)
		}
	}
}

// give me a func, and the func will return a error, if error is nil, reset retry time else retry
func (bt *BaseTracker) RetrySleep(err error) error {
	// if no error, reset retry time
	if err == nil {
		bt.retrySleepTime = bt.retryInitSleepTime
		bt.currentRetry = 0
		return nil
	}
	// if max retry reached, return error
	if bt.currentRetry >= bt.maxRetry {
		return fmt.Errorf("max retry reached")
	}

	bt.currentRetry++
	time.Sleep(time.Duration(bt.retrySleepTime) * time.Second)

	// 1h
	maxSleepTime := 3600
	bt.retrySleepTime *= 2
	if bt.retrySleepTime > maxSleepTime {
		bt.retrySleepTime = maxSleepTime
	}

	return nil
}

func (bt *BaseTracker) LogError(message string, err error, fields ...zap.Field) {
	fields = append(fields, zap.String("Tracker", bt.String()))
	fields = append(fields, zap.Int("SleepTime", bt.retrySleepTime))
	fields = append(fields, zap.Error(err))
	log.Error(message, fields...)
}

func (bt *BaseTracker) LogWarn(message string, fields ...zap.Field) {
	fields = append(fields, zap.String("Tracker", bt.String()))
	fields = append(fields, zap.Int("SleepTime", bt.retrySleepTime))
	log.Warn(message, fields...)
}

func (bt *BaseTracker) LogInfo(message string, fields ...zap.Field) {
	fields = append(fields, zap.String("Tracker", bt.String()))
	fields = append(fields, zap.Int("SleepTime", bt.retrySleepTime))
	log.Info(message, fields...)
}

func (bt *BaseTracker) LogDebug(message string, fields ...zap.Field) {
	fields = append(fields, zap.String("Tracker", bt.String()))
	fields = append(fields, zap.Int("SleepTime", bt.retrySleepTime))
	log.Debug(message, fields...)
}

func (bt *BaseTracker) String() string {
	return fmt.Sprintf("%s token=%s", bt.trackerName, bt.tokenAddress)
}

func (bt *BaseTracker) GetHttpClient() (*rpc.Client, error) {
	// TODO if client failed, retry different endpoint
	rpcClient := rpc.New(bt.httpEndpoint[0])

	bt.rpcClient = rpcClient
	return rpcClient, nil
}

func (bt *BaseTracker) GetWsClient() (*ws.Client, error) {
	// TODO if client failed, retry different endpoint
	wsClient, err := ws.Connect(context.Background(), bt.wsEndpoint[0])
	if err != nil {
		return nil, err
	}

	bt.wsClient = wsClient
	return wsClient, nil
}

func NewBaseTracker(httpEndpoint []string, wsEndpoint []string, tokenAddress string, trackerName string) (*BaseTracker, error) {
	if (len(httpEndpoint) == 0) || (len(wsEndpoint) == 0) {
		return nil, fmt.Errorf("httpEndpoint and wsEndpoint must not be empty")
	}

	channelBufferSize := 1000

	tracker := &BaseTracker{
		httpEndpoint:       httpEndpoint,
		wsEndpoint:         wsEndpoint,
		tokenAddress:       tokenAddress,
		trackerName:        trackerName,
		channelBufferSize:  channelBufferSize,
		retryInitSleepTime: 3,
		retrySleepTime:     3,
		// ~= 1day
		maxRetry:     240,
		tradeChannel: make(chan *btype.Trade, channelBufferSize),
	}
	_, err := tracker.GetWsClient()
	if err != nil {
		return nil, fmt.Errorf("Error getting ws client: %v", err)
	}

	_, err = tracker.GetHttpClient()
	if err != nil {
		return nil, fmt.Errorf("Error getting http client: %v", err)
	}

	return tracker, nil
}

func (bt *BaseTracker) Close() error {
	close(bt.tradeChannel)
	bt.wsClient.Close()

	return bt.rpcClient.Close()
}

func (bt *BaseTracker) parseTrade(ctx context.Context, transaction rpc.TransactionParsed, slot uint64) (*btype.Trade, error) {
	parser, err := tokentradeparser.NewTransactionParserFromTransaction(transaction.Transaction, transaction.Meta)
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

	// TODO the string func using parent or inherited struct
	return btype.NewTrade(bt.String(), swapData, slot), nil
}
