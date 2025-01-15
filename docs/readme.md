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

### 2025-1-14

Roles:

- Client: 解决 ratelimit、rpc node 不稳定问题。自动切换新的 endpoint，自动添加 proxy，自动变更 useragent，自动变更 apikey 等
- Tracker: outpust is trades
- Handler: input is trades
  - 直接发往 kafka
  - 添加一些新的字段
  - 过滤部分 trades
  - ...
- Dispatcher: 保证 trades 全部被正确处理，记录进度，当程序重启的时候能从指定进度启动
  - Job Management:

    - read trades of a slot and send the trades to kafka, if failed then retry. the job will complete
    - read trade event and send the trade to kafka. the job will not complete, but can get progress
  - Task Management:
    - get a token's history trade and real-time trades.
      - get the token's history trade
      - get the token's real-time trade
      - if the progress restarted, get the recent history trades and get the real-time trade
