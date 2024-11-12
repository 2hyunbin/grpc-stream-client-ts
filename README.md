### Setup
You first need a full node with [gRPC streaming enabled](https://docs.dydx.exchange/api_integration-full-node-streaming#enabling-streaming).

1. Install dependencies:
    ```bash
    npm install
    ```

2. Create a `.env` file 
   ```
   INDEXER_API=https://indexer.dydx.trade
   FULL_NODE_HOST=

   CLOB_PAIR_IDS=0 1
   SUBACCOUNT_IDS=

   USE_GRPC= true or empty
   GRPC_PORT=

   USE_WEBSOCKET= true or empty
   WEBSOCKET_PORT=

   INTERVAL_MS=

   PRINT_BOOKS= true or empty
   PRINT_FILLS= true or empty
   PRINT_ACCOUNTS= true or empty
   PRINT_TAKER_ORDERS= true or empty
   ```

3. Build & Start
    ```bash
    npm run build
    npm start 
    ```

### Book Streaming Example

Example output:
