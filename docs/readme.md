# readme

## Feature

- [ ] add simple web dashboard for monitor, management and debug
- [ ] user can get all trade of a token

## Development

### 2025-1-10

- [ ] track history trade of a token
- [ ] track real-time trade of a token

- [ ] token-trade-tracker
  - [ ] get history transactions of a token
  - [ ] watch latest transaction of a token
  - [ ] decode the transaction into multi trades

### 2025-1-13

- 432,000 slots per epoch, 每个 epoch 决定每个 slot 都由谁出块
- 对应的 validator 会在每个 slot 接收消息并出块
- 使用 `rpc.CommitmentConfirmed`, `rpc.CommitmentProcessed` 差不多会有 1s 的延时
- endpoint provider:
  - <https://drpc.org/>
  - <https://dashboard.alchemy.com/> only support https
  - <https://dashboard.helius.dev/>
