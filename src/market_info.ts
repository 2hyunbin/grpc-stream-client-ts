import axios from "axios";

const USDC_ATOMIC_RESOLUTION = -6;

interface MarketInfo {
  atomicResolution: number;
  quantumConversionExponent: number;
  // add any other properties that might be present in market info
}

export async function queryMarketInfo(
  indexerApi: string,
): Promise<Record<number, MarketInfo>> {
  const uri = `${indexerApi}/v4/perpetualMarkets`;
  try {
    const response = await axios.get(uri);
    if (response.status !== 200) {
      throw new Error(
        `Failed to query markets from ${uri}: ${response.statusText}`,
      );
    }

    const markets = response.data.markets;
    const marketInfo: Record<number, MarketInfo> = {};

    for (const key in markets) {
      const market = markets[key];
      const marketId = parseInt(market.clobPairId, 10);
      marketInfo[marketId] = {
        atomicResolution: market.atomicResolution,
        quantumConversionExponent: market.quantumConversionExponent,
        // map any other required properties
      };
    }

    return marketInfo;
  } catch (error) {
    throw new Error(`Error querying market info: ${error}`);
  }
}

export function quantumsToSize(
  quantums: number,
  atomicResolution: number,
): number {
  /**
   * Convert quantums to a human-readable size.
   */
  return quantums / Math.pow(10, -atomicResolution);
}

export function subticksToPrice(
  subticks: number,
  atomicResolution: number,
  quantumConversionExponent: number,
): number {
  /**
   * Convert subticks to a human-readable price.
   */
  const exponent =
    atomicResolution - quantumConversionExponent - USDC_ATOMIC_RESOLUTION;
  return subticks / Math.pow(10, exponent);
}
