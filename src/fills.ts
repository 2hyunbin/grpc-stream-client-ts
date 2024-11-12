import { ClobMatch } from "@dydxprotocol/v4-proto/src/codegen/dydxprotocol/clob/matches";
import {
  Order,
  OrderId as protoOrderId,
} from "@dydxprotocol/v4-proto/src/codegen/dydxprotocol/clob/order";
import { StreamOrderbookFill } from "@dydxprotocol/v4-proto/src/codegen/dydxprotocol/clob/query";
import { SubaccountId } from "@dydxprotocol/v4-proto/src/codegen/dydxprotocol/subaccounts/subaccount";
import { Long } from "@grpc/proto-loader";

enum FillType {
  NORMAL = "NORMAL",
  LIQUIDATION = "LIQUIDATION",
  DELEVERAGING = "DELEVERAGING",
}

interface OrderId {
  ownerAddress: string;
  subaccountNumber: number;
  clientId: number;
  orderFlags: number;
}

export interface Fill {
  clobPairId: number;
  maker: OrderId;
  taker: OrderId;
  quantums: Long;
  subticks: Long;
  takerIsBuy: boolean;
  execMode: number;
  fillType: FillType;
  makerTotalFilledQuantums?: Long;
}

export function parseFill(
  orderFill: StreamOrderbookFill,
  execMode: number,
): Fill[] {
  const clobMatch = orderFill.clobMatch;

  if (!clobMatch) {
    throw new Error("clobMatch is undefined in parseFill");
  }

  const orderStatesAtFillTime: Record<string, [Order, Long]> = {};
  orderFill.orders.forEach((o, index) => {
    if (!o.orderId) {
      throw new Error("o.orderId is undefined in parseFill");
    }
    const orderId = parsePbId(o.orderId);
    orderStatesAtFillTime[orderId.clientId] = [o, orderFill.fillAmounts[index]];
  });

  if (clobMatch.matchOrders) {
    return parseFills(execMode, clobMatch, orderStatesAtFillTime);
  } else if (clobMatch.matchPerpetualLiquidation) {
    return parseLiquidations(execMode, clobMatch, orderStatesAtFillTime);
  } else if (clobMatch.matchPerpetualDeleveraging) {
    return parseDeleveragings(execMode, clobMatch);
  }

  return [];
}

function parsePbId(oid: protoOrderId): OrderId {
  if (
    !oid.subaccountId ||
    !oid.subaccountId.owner ||
    !oid.subaccountId.number
  ) {
    throw new Error("oid.subaccountId is undefined in parseFill");
  }

  return {
    ownerAddress: oid.subaccountId.owner,
    subaccountNumber: oid.subaccountId.number,
    clientId: oid.clientId,
    orderFlags: oid.orderFlags,
  };
}

function parseAccId(accId: SubaccountId): OrderId {
  return {
    ownerAddress: accId.owner,
    subaccountNumber: accId.number,
    clientId: 0,
    orderFlags: 0,
  };
}

function parseFills(
  execMode: number,
  clobMatch: ClobMatch,
  orderStatesAtFillTime: Record<string, [Order, Long]>,
): Fill[] {
  if (!clobMatch.matchOrders?.takerOrderId) {
    throw new Error(
      "clobMatch.matchOrders.takerOrderId is undefined in parseFill",
    );
  }

  if (!clobMatch.matchOrders?.fills) {
    throw new Error(
      "clobMatch.matchOrders.takerOrderId is undefined in parseFill",
    );
  }

  const fills: Fill[] = [];
  const takerId = parsePbId(clobMatch.matchOrders.takerOrderId);

  clobMatch.matchOrders.fills.forEach((fill) => {
    if (!fill.makerOrderId)
      throw new Error("fill.makerOrderId is undefined in parseFills");

    const makerId = parsePbId(fill.makerOrderId);
    const [maker, tfa] = orderStatesAtFillTime[makerId.clientId];
    if (!maker.orderId)
      throw new Error("maker.orderId is undefined in parseFills");
    fills.push({
      clobPairId: maker.orderId.clobPairId,
      maker: makerId,
      taker: takerId,
      makerTotalFilledQuantums: tfa,
      quantums: fill.fillAmount,
      subticks: maker.subticks,
      takerIsBuy: maker.side === 2,
      execMode,
      fillType: FillType.NORMAL,
    });
  });

  return fills;
}

function parseLiquidations(
  execMode: number,
  clobMatch: ClobMatch,
  orderStatesAtFillTime: Record<string, [Order, Long]>,
): Fill[] {
  const fills: Fill[] = [];
  if (!clobMatch.matchPerpetualLiquidation)
    throw new Error(
      "clobMatch.matchPerpetualLiquidation is undefined in parseLiquidations",
    );

  if (!clobMatch.matchPerpetualLiquidation.liquidated)
    throw new Error(
      "clobMatch.matchPerpetualLiquidation.liquidated is undefined in parseLiquidations",
    );

  const liquidatedId = parseAccId(
    clobMatch.matchPerpetualLiquidation.liquidated,
  );

  clobMatch.matchPerpetualLiquidation.fills.forEach((fill) => {
    if (!clobMatch.matchPerpetualLiquidation)
      throw new Error(
        "clobMatch.matchPerpetualLiquidation is undefined in parseLiquidations",
      );
    if (!fill.makerOrderId)
      throw new Error("fill.makerOrderId is undefined in parseLiquidations");
    const makerId = parsePbId(fill.makerOrderId);
    const [maker, tfa] = orderStatesAtFillTime[makerId.clientId];
    if (!maker.orderId)
      throw new Error("maker.orderId is undefined in parseLiquidations");
    fills.push({
      clobPairId: maker.orderId.clobPairId,
      maker: makerId,
      taker: liquidatedId,
      makerTotalFilledQuantums: tfa,
      quantums: fill.fillAmount,
      subticks: maker.subticks,
      takerIsBuy: clobMatch.matchPerpetualLiquidation.isBuy,
      execMode,
      fillType: FillType.LIQUIDATION,
    });
  });

  return fills;
}

function parseDeleveragings(execMode: number, clobMatch: ClobMatch): Fill[] {
  if (!clobMatch.matchPerpetualDeleveraging)
    throw new Error(
      "clobMatch.matchPerpetualDeleveraging is undefined in parseDeleveragings",
    );
  if (!clobMatch.matchPerpetualDeleveraging.liquidated)
    throw new Error(
      "clobMatch.matchPerpetualDeleveraging.liquidated is undefined in parseDeleveragings",
    );

  const fills: Fill[] = [];
  const liquidatedId = parseAccId(
    clobMatch.matchPerpetualDeleveraging.liquidated,
  );

  clobMatch.matchPerpetualDeleveraging.fills.forEach((fill) => {
    if (!clobMatch.matchPerpetualDeleveraging)
      throw new Error(
        "clobMatch.matchPerpetualDeleveraging is undefined in parseDeleveragings",
      );
    if (!fill.offsettingSubaccountId)
      throw new Error(
        "fill.offsettingSubaccountId is undefined in parseDeleveragings",
      );
    fills.push({
      clobPairId: clobMatch.matchPerpetualDeleveraging.perpetualId,
      maker: parseAccId(fill.offsettingSubaccountId),
      taker: liquidatedId,
      quantums: fill.fillAmount,
      subticks: Long.fromNumber(0),
      takerIsBuy: false,
      execMode,
      fillType: FillType.DELEVERAGING,
    });
  });

  return fills;
}
