import {
  StreamOrderbookFill,
  StreamOrderbookUpdate,
  StreamOrderbookUpdatesResponse,
  StreamTakerOrder,
} from "@dydxprotocol/v4-proto/src/codegen/dydxprotocol/clob/query";
import { Fill, parseFill } from "./fills";
import {
  OrderPlaceV1,
  OrderRemoveV1,
  OrderUpdateV1,
} from "@dydxprotocol/v4-proto/src/codegen/dydxprotocol/indexer/off_chain_updates/off_chain_updates";
import {
  parseSubaccounts,
  StreamSubaccount,
  SubaccountId,
} from "./subaccounts";
import TakerOrderMetrics from "./take_order_metrics";
import { StreamSubaccountUpdate } from "@dydxprotocol/v4-proto/src/codegen/dydxprotocol/subaccounts/streaming";
import { LimitOrderBook } from "./book";
import { isEqual } from "lodash";
import {
  parseIndexerOid,
  parseIndexerOrder,
  parseProtocolOrder,
} from "./helpers";
import Long from "long";

export abstract class FeedHandler {
  abstract handle(message: StreamOrderbookUpdatesResponse): Fill[];
  abstract getBooks(): Record<number, LimitOrderBook>;
  abstract getSubaccounts(): Record<string, StreamSubaccount>;
  abstract getRecentSubaccountUpdates(): Record<string, StreamSubaccount>;
}

export class StandardFeedHandler extends FeedHandler {
  private books: Record<number, LimitOrderBook> = {};
  private heights: Record<number, number> = {};
  private hasSeenFirstSnapshot = false;
  private takerOrderMetrics = new TakerOrderMetrics();
  private subaccounts: Record<string, StreamSubaccount> = {};
  private updatedSubaccounts: SubaccountId[] = [];

  handle(message: StreamOrderbookUpdatesResponse): Fill[] {
    const collectedFills: Fill[] = [];
    this.updatedSubaccounts = [];

    for (const update of message.updates) {
      const height = update.blockHeight;

      if (update.orderbookUpdate) {
        this.handleOrderbookUpdate(update.orderbookUpdate, height);
        break;
      } else if (update.orderFill) {
        const fs = this.handleFills(update.orderFill, update.execMode);
        if (fs.length > 0) {
          this.updateHeight(fs[0].clobPairId, height);
        }
        collectedFills.push(...fs);
        break;
      } else if (update.takerOrder) {
        this.handleTakerOrder(update.takerOrder, height);
        break;
      } else if (update.subaccountUpdate) {
        this.handleSubaccounts(update.subaccountUpdate);
        break;
      }
    }

    return collectedFills;
  }

  private updateHeight(clobPairId: number, newBlockHeight: number) {
    if (newBlockHeight <= 0) {
      throw new Error(`Invalid block height: ${newBlockHeight}`);
    }
    if (
      !this.heights[clobPairId] ||
      newBlockHeight >= this.heights[clobPairId]
    ) {
      this.heights[clobPairId] = newBlockHeight;
    } else {
      throw new Error(
        `Block height decreased from ${this.heights[clobPairId]} to ${newBlockHeight}`,
      );
    }
  }

  private handleSubaccounts(update: StreamSubaccountUpdate) {
    const parsedSubaccount = parseSubaccounts(update);
    const subaccountId = parsedSubaccount.subaccountId;
    this.updatedSubaccounts.push(subaccountId);

    if (update.snapshot) {
      if (this.subaccounts[subaccountId.ownerAddress]) {
        console.warn(
          `Saw multiple snapshots for subaccount id ${subaccountId}`,
        );
        this.updatedSubaccounts = this.updatedSubaccounts.filter((id) =>
          isEqual(id, subaccountId),
        );
        return;
      }
      this.subaccounts[subaccountId.ownerAddress] = parsedSubaccount;
    } else {
      if (!this.subaccounts[subaccountId.ownerAddress]) return;

      const existingSubaccount = this.subaccounts[subaccountId.ownerAddress];
      existingSubaccount.perpetualPositions = {
        ...existingSubaccount.perpetualPositions,
        ...parsedSubaccount.perpetualPositions,
      };
      existingSubaccount.assetPositions = {
        ...existingSubaccount.assetPositions,
        ...parsedSubaccount.assetPositions,
      };
    }
  }

  private handleTakerOrder(order: StreamTakerOrder, blockHeight: number) {
    if (!order.order) {
      throw new Error(`order.order is undefined in handleTakerOrder`);
    }

    const parsedOrder = parseProtocolOrder(order.order);
    this.takerOrderMetrics.processOrder(parsedOrder, blockHeight);
  }

  private handleFills(
    orderFill: StreamOrderbookFill,
    execMode: number,
  ): Fill[] {
    if (!this.hasSeenFirstSnapshot) return [];

    const fs = parseFill(orderFill, execMode);
    for (const fill of fs) {
      if (!fill.makerTotalFilledQuantums) {
        throw new Error(
          `fill.makerTotalFilledQuantum is undefined in handleFills`,
        );
      }
      const order = this.getBook(fill.clobPairId).getOrder(fill.maker);
      if (order) {
        order.quantums = order.originalQuantums.subtract(
          fill.makerTotalFilledQuantums,
        );
      }
    }
    return fs;
  }

  private handleOrderbookUpdate(
    update: StreamOrderbookUpdate,
    blockHeight: number,
  ) {
    if (!this.hasSeenFirstSnapshot && !update.snapshot) return;

    if (update.snapshot) {
      if (this.hasSeenFirstSnapshot) {
        console.warn("Skipping subsequent snapshot.");
        return;
      }
      this.hasSeenFirstSnapshot = true;
    }

    for (const u of update.updates) {
      let cpid: number | undefined;

      if (u.orderPlace) {
        cpid = this.handleOrderPlace(u.orderPlace);
        break;
      } else if (u.orderUpdate) {
        cpid = this.handleOrderUpdate(u.orderUpdate);
        break;
      } else if (u.orderRemove) {
        cpid = this.handleOrderRemove(u.orderRemove);
        break;
      }

      if (cpid !== undefined) {
        this.updateHeight(cpid, blockHeight);
      }
    }

    this.validateBooks();
  }

  private validateBooks() {
    for (const [cpid, book] of Object.entries(this.books)) {
      const ask = book.asks().next().value;
      const bid = book.bids().next().value;

      if (ask && bid) {
        const pAsk = ask.subticks;
        const pBid = bid.subticks;
        if (pAsk <= pBid) {
          throw new Error(
            `Ask price ${pAsk} <= bid price ${pBid} for clob pair ${cpid}`,
          );
        }
      }
    }
  }

  private getBook(clobPairId: number): LimitOrderBook {
    if (!this.books[clobPairId]) {
      this.books[clobPairId] = new LimitOrderBook();
    }
    return this.books[clobPairId];
  }

  private handleOrderPlace(orderPlace: OrderPlaceV1): number {
    if (!orderPlace.order) {
      throw new Error("orderPlace.order in undefined in handleOrderPlace");
    }
    const order = parseIndexerOrder(orderPlace.order);
    const clobPairId = orderPlace.order?.orderId?.clobPairId;
    if (!clobPairId) throw new Error("Missing CLOB pair ID");

    const book = this.getBook(clobPairId);
    if (book.getOrder(order.orderId)) {
      throw new Error(`Order ${order.orderId} already exists in the book`);
    }

    book.addOrder(order);
    return clobPairId;
  }

  private handleOrderUpdate(orderUpdate: OrderUpdateV1): number {
    const clobPairId = orderUpdate.orderId?.clobPairId;
    if (!clobPairId) throw new Error("Missing CLOB pair ID");
    if (!orderUpdate.orderId)
      throw new Error("orderUpdate.orderId is undefined in handleOrderUpdate");
    const oid = parseIndexerOid(orderUpdate.orderId);
    const order = this.getBook(clobPairId).getOrder(oid);

    if (!order) return clobPairId;

    order.quantums = order.originalQuantums.subtract(
      orderUpdate.totalFilledQuantums,
    );
    return clobPairId;
  }

  private handleOrderRemove(orderRemove: OrderRemoveV1): number {
    const clobPairId = orderRemove.removedOrderId?.clobPairId;
    if (!clobPairId) throw new Error("Missing CLOB pair ID");

    if (!orderRemove.removedOrderId)
      throw new Error(
        "orderRemove.removedOrderId is undefined in handelOrderRemove",
      );
    const oid = parseIndexerOid(orderRemove.removedOrderId);
    const book = this.getBook(clobPairId);

    if (book.getOrder(oid)) {
      book.removeOrder(oid);
    } else {
      throw new Error(`Order ${oid} not in the book`);
    }

    return clobPairId;
  }

  getBooks(): Record<number, LimitOrderBook> {
    return this.books;
  }

  getSubaccounts(): Record<string, StreamSubaccount> {
    return this.subaccounts;
  }

  getRecentSubaccountUpdates(): Record<string, StreamSubaccount> {
    return Object.fromEntries(
      this.updatedSubaccounts
        .map((id) => [id, this.subaccounts[id.ownerAddress]])
        .filter(([_, subaccount]) => subaccount),
    );
  }
}
