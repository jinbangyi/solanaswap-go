package tokentradetracker

// import (
// 	"context"
// 	"testing"

// 	"github.com/gagliardetto/solana-go"
// 	"github.com/gagliardetto/solana-go/rpc"
// 	"github.com/gagliardetto/solana-go/rpc/ws"
// 	"github.com/stretchr/testify/assert"
// 	"github.com/stretchr/testify/mock"
// )

// // Mocking the ws.Client and rpc.Client
// type MockWsClient struct {
// 	mock.Mock
// }

// func (m *MockWsClient) LogsSubscribeMentions(pubkey solana.PublicKey, commitment rpc.CommitmentType) (*ws.LogsSubscription, error) {
// 	args := m.Called(pubkey, commitment)
// 	return args.Get(0).(*ws.LogsSubscription), args.Error(1)
// }

// type MockRpcClient struct {
// 	mock.Mock
// }

// func (m *MockRpcClient) GetTransaction(ctx context.Context, signature solana.Signature, opts *rpc.GetTransactionOpts) (*rpc.TransactionWithMeta, error) {
// 	args := m.Called(ctx, signature, opts)
// 	return args.Get(0).(*rpc.TransactionWithMeta), args.Error(1)
// }

// func TestNewRealTimeTracker(t *testing.T) {
// 	httpEndpoint := []string{"http://localhost:8899"}
// 	wsEndpoint := []string{"ws://localhost:8900"}

// 	tracker, err := NewRealTimeTracker(httpEndpoint, wsEndpoint)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, tracker)
// }

// func TestRealTimeTracker_GetHttpClient(t *testing.T) {
// 	httpEndpoint := []string{"http://localhost:8899"}
// 	wsEndpoint := []string{"ws://localhost:8900"}

// 	tracker, err := NewRealTimeTracker(httpEndpoint, wsEndpoint)
// 	assert.NoError(t, err)

// 	client, err := tracker.GetHttpClient()
// 	assert.NoError(t, err)
// 	assert.NotNil(t, client)
// }

// func TestRealTimeTracker_GetWsClient(t *testing.T) {
// 	httpEndpoint := []string{"http://localhost:8899"}
// 	wsEndpoint := []string{"ws://localhost:8900"}

// 	tracker, err := NewRealTimeTracker(httpEndpoint, wsEndpoint)
// 	assert.NoError(t, err)

// 	client, err := tracker.GetWsClient()
// 	assert.NoError(t, err)
// 	assert.NotNil(t, client)
// }

// func TestRealTimeTracker_ReadTrade(t *testing.T) {
// 	httpEndpoint := []string{"http://localhost:8899"}
// 	wsEndpoint := []string{"ws://localhost:8900"}

// 	tracker, err := NewRealTimeTracker(httpEndpoint, wsEndpoint)
// 	assert.NoError(t, err)

// 	mockWsClient := new(MockWsClient)
// 	mockRpcClient := new(MockRpcClient)

// 	tracker.wsClient = mockWsClient
// 	tracker.rpcClient = mockRpcClient

// 	logsSubscription := &ws.LogsSubscription{}
// 	mockWsClient.On("LogsSubscribeMentions", mock.Anything, rpc.CommitmentConfirmed).Return(logsSubscription, nil)

// 	tradeChannel, err := tracker.ReadTrade()
// 	assert.NoError(t, err)
// 	assert.NotNil(t, tradeChannel)
// }

// func TestRealTimeTracker_GetTrade(t *testing.T) {
// 	httpEndpoint := []string{"http://localhost:8899"}
// 	wsEndpoint := []string{"ws://localhost:8900"}

// 	tracker, err := NewRealTimeTracker(httpEndpoint, wsEndpoint)
// 	assert.NoError(t, err)

// 	mockRpcClient := new(MockRpcClient)
// 	tracker.rpcClient = mockRpcClient

// 	logResult := &ws.LogResult{
// 		Value: ws.LogResultValue{
// 			Signature: solana.Signature{},
// 		},
// 	}

// 	tx := &rpc.TransactionWithMeta{}
// 	mockRpcClient.On("GetTransaction", mock.Anything, mock.Anything, mock.Anything).Return(tx, nil)

// 	trade, err := tracker.GetTrade(logResult)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, trade)
// }
