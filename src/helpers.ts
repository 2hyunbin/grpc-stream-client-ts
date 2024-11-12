import { OffChainUpdateV1 } from "@dydxprotocol/v4-proto/src/codegen/dydxprotocol/indexer/off_chain_updates/off_chain_updates";
import { StreamUpdate } from "@dydxprotocol/v4-proto/src/codegen/dydxprotocol/clob/query";
import {
  IndexerOrder,
  IndexerOrderId,
} from "@dydxprotocol/v4-proto/src/codegen/dydxprotocol/indexer/protocol/v1/clob";
import { OrderId as bookOrderId, Order as bookOrder } from "./book";
import { Order } from "@dydxprotocol/v4-proto/src/codegen/dydxprotocol/clob/order";

function getClobPairIdFromOffchainUpdate(update: OffChainUpdateV1): number {
  let clobPairId: number | null = null;

  if (update.orderPlace) {
    clobPairId = update.orderPlace!.order!.orderId!.clobPairId;
  } else if (update.orderUpdate) {
    clobPairId = update.orderUpdate!.orderId!.clobPairId;
  } else if (update.orderRemove) {
    clobPairId = update.orderRemove!.removedOrderId!.clobPairId;
  } else {
    throw new Error(`Unknown update type in: ${update}`);
  }
  return clobPairId!;
}

export function parseIndexerOid(oidFields: IndexerOrderId): bookOrderId {
  if (!oidFields.subaccountId) {
    throw new Error(`oidFields.subaccountId is undefined in parseIndexerOid`);
  }

  return {
    ownerAddress: oidFields.subaccountId.owner,
    subaccountNumber: oidFields.subaccountId.number,
    clientId: oidFields.clientId,
    orderFlags: oidFields.orderFlags,
  };
}

export function parseIndexerOrder(order: IndexerOrder): bookOrder {
  if (!order.orderId) {
    throw new Error(`order.orderId is undefined in parseIndexerOid`);
  }

  const lobOid = parseIndexerOid(order.orderId);
  return {
    orderId: lobOid,
    isBid: order.side === 1,
    originalQuantums: order.quantums,
    quantums: order.quantums,
    subticks: order.subticks,
  };
}

export function parseProtocolOrder(order: Order): bookOrder {
  if (!order.orderId) {
    throw new Error(`order.orderId is undefined in parseProtocolOrder`);
  }

  return {
    orderId: parseIndexerOid(order.orderId),
    isBid: order.side === 1,
    originalQuantums: order.quantums,
    quantums: order.quantums,
    subticks: order.subticks,
  };
}

function isSnapshotUpdate(update: StreamUpdate): boolean {
  if (update.orderbookUpdate) {
    return update.orderbookUpdate!.snapshot;
  } else if (update.orderFill || update.takerOrder) {
    return false;
  } else if (update.subaccountUpdate) {
    return update.subaccountUpdate!.snapshot;
  } else {
    throw new Error(`Unknown update type in: ${update}`);
  }
}
