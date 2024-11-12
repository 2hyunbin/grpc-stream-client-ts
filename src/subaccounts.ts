import { StreamSubaccountUpdate } from "@dydxprotocol/v4-proto/src/codegen/dydxprotocol/subaccounts/streaming";

export interface SubaccountId {
  ownerAddress: string;
  subaccountNumber: number;
}

interface SubaccountPerpetualPosition {
  perpetualId: number;
  quantums: number;
}

interface SubaccountAssetPosition {
  assetId: number;
  quantums: number;
}

export interface StreamSubaccount {
  subaccountId: SubaccountId;
  perpetualPositions: Map<number, SubaccountPerpetualPosition>;
  assetPositions: Map<number, SubaccountAssetPosition>;
}

export function parseSubaccounts(
  streamSubaccountUpdate: StreamSubaccountUpdate,
): StreamSubaccount {
  if (!streamSubaccountUpdate.subaccountId) {
    throw new Error(
      "StreamSubaccountUpdate.subaccountId is undefined in parseSubaccounts",
    );
  }

  const subaccountId: SubaccountId = {
    ownerAddress: streamSubaccountUpdate.subaccountId.owner,
    subaccountNumber: streamSubaccountUpdate.subaccountId.number,
  };

  const perpetualPositions = new Map<number, SubaccountPerpetualPosition>();
  streamSubaccountUpdate.updatedPerpetualPositions.forEach((pos: any) => {
    perpetualPositions.set(pos.perpetualId, {
      perpetualId: pos.perpetualId,
      quantums: pos.quantums,
    });
  });

  const assetPositions = new Map<number, SubaccountAssetPosition>();
  streamSubaccountUpdate.updatedAssetPositions.forEach((pos: any) => {
    assetPositions.set(pos.assetId, {
      assetId: pos.assetId,
      quantums: pos.quantums,
    });
  });

  return {
    subaccountId: subaccountId,
    perpetualPositions: perpetualPositions,
    assetPositions: assetPositions,
  };
}
