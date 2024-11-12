import { Order } from "./book";

class TakerOrderMetrics {
  private blockHeight: number = 0;
  private numOrders: number = 0;
  private bids: number = 0;
  private asks: number = 0;
  private orderFlags: Record<number, number> = {};

  processOrder(order: Order, blockHeight: number): void {
    // Print out metrics if block height increases
    if (blockHeight !== this.blockHeight && blockHeight !== 0) {
      this.print();
      this.flush();
    }
    this.blockHeight = blockHeight;

    if (order.isBid) {
      this.bids += 1;
    } else {
      this.asks += 1;
    }

    const orderFlag = order.orderId.orderFlags;
    this.orderFlags[orderFlag] = (this.orderFlags[orderFlag] || 0) + 1;
  }

  flush(): void {
    this.blockHeight = 0;
    this.numOrders = 0;
    this.bids = 0;
    this.asks = 0;
    this.orderFlags = {};
  }

  print(): void {
    if (process.env.PRINT_TAKER_ORDERS === "true") {
      return;
    }

    const numShort = this.orderFlags[0] || 0;
    const numConditional = this.orderFlags[32] || 0;
    const numLongTerm = this.orderFlags[64] || 0;

    console.info(
      `Block ${this.blockHeight}: ${this.numOrders} Taker orders, ${this.bids} bids, ${this.asks} asks, ` +
        `${numShort} short term, ${numLongTerm} long term, ${numConditional} conditional orders.`,
    );
  }
}

export default TakerOrderMetrics;
