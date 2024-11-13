import {
  quantumsToSize,
  queryMarketInfo,
  subticksToPrice,
} from "./market_info";
import * as process from "node:process";
import WebSocket from "ws";
import {
  StreamOrderbookUpdatesRequest,
  StreamOrderbookUpdatesResponse,
} from "@dydxprotocol/v4-proto/src/codegen/dydxprotocol/clob/query";
import { Fill } from "./fills";
import { StreamSubaccount } from "./subaccounts";
import { LimitOrderBook } from "./book";
import { FeedHandler, StandardFeedHandler } from "./feed_handler";
import { createRPCQueryClient } from "@dydxprotocol/v4-proto/src/codegen/dydxprotocol/rpc.query";
import * as path from "node:path";

require("dotenv").config();

async function main() {
  if (process.env.INDEXER_API === undefined) {
    throw new Error("INDEXER_API is not in .env");
  }

  if (process.env.FULL_NODE_HOST === undefined) {
    throw new Error("INDEXER_API is not in .env");
  }

  if (process.env.CLOB_PAIR_IDS === undefined) {
    throw new Error("CLOB_PAIR_IDS is not in .env");
  }

  if (process.env.SUBACCOUNT_IDS === undefined) {
    throw new Error("SUBACCOUNT_IDS is not in .env");
  }

  if (process.env.USE_GRPC !== "true" && process.env.USE_WEBSOCKET !== "true") {
    throw new Error("Either use grpc or use websocket must be true");
  }

  if (process.env.USE_GRPC === "true") {
    if (process.env.GRPC_PORT === undefined) {
      throw new Error("To use gRPC, specify the port in the .env file.");
    }
  }

  if (process.env.USE_WEBSOCKET === "true") {
    if (process.env.WEBSOCKET_PORT === undefined) {
      throw new Error("To use websocket, specify the port in the .env file.");
    }
  }

  if (process.env.INTERVAL_MS === undefined) {
    throw new Error("INTERVAL_MS is not in .env");
  }

  const cpidToMarketInfo = await queryMarketInfo(process.env.INDEXER_API);
  console.log(cpidToMarketInfo);

  const host = process.env.FULL_NODE_HOST;
  const cpids = process.env.CLOB_PAIR_IDS.split(" ").map(Number);
  const subaccountIds = process.env.SUBACCOUNT_IDS.split(" ");

  // This manages order book state
  let feedHandler: FeedHandler = new StandardFeedHandler();

  if (process.env.USE_GRPC === "true") {
    const grpc = require("@grpc/grpc-js");
    const protoLoader = require("@grpc/proto-loader"); // protoLoader 임포트
    const path = require("path");

    const grpcPort = process.env.GRPC_PORT;
    const grpcAddr = `${host}:${26657}`;

    // Load gRPC connection and start listening
    const client = (
      await createRPCQueryClient({
        rpcEndpoint: grpcAddr,
      })
    ).dydxprotocol.clob;

    // console.log(await client.clobPair({ id:  }));

    await client.streamOrderbookUpdates({
      clobPairId: [0],
      subaccountIds: [{ owner: "1", number: 0 }],
    });
    const interval = Number(process.env.INTERVAL_MS || "1000");

    const tasks = [
      listenToGrpcStream(
        client,
        cpids,
        subaccountIds,
        cpidToMarketInfo,
        feedHandler,
      ),
    ];

    if (process.env.PRINT_BOOKS === "true") {
      const printBooksTask = printBooksEveryNMs(
        feedHandler,
        cpidToMarketInfo,
        interval,
      );
      tasks.push(printBooksTask);
    }

    await Promise.all(tasks);
  } else if (process.env.USE_WEBSOCKET === "true") {
    const params: string[] = [];
    if (cpids) {
      const joinedCpids = cpids.map(String).join(",");
      params.push(`clobPairIds=${joinedCpids}`);
    }

    if (subaccountIds) {
      const joinedSubaccountIds = subaccountIds.map(String).join(",");
      params.push(`subaccountIds=${joinedSubaccountIds}`);
    }

    const websocketPort = process.env.WEBSOCKET_PORT;
    const paramsStr = params.join("&");
    const websocketAddr = `ws://${host}:${websocketPort}/ws?${paramsStr}`;

    const websocket = new WebSocket(websocketAddr);
    websocket.on("open", async () => {
      const interval = Number(process.env.INTERVAL_MS || "1000");
      const tasks = [
        listenToWebSocket(websocket, cpidToMarketInfo, feedHandler),
      ];

      if (process.env.PRINT_BOOKS === "true") {
        const printBooksTask = printBooksEveryNMs(
          feedHandler,
          cpidToMarketInfo,
          interval,
        );
        tasks.push(printBooksTask);
      }

      await Promise.all(tasks);
    });

    websocket.on("error", (error: any) => {
      console.error("WebSocket connection error:", error);
    });
  } else {
    console.error("Must specify use_grpc or use_websocket in .env");
  }
}

async function listenToGrpcStream(
  client: any,
  clobPairIds: number[],
  subaccountIds: string[],
  cpidToMarketInfo: Record<number, any>,
  feedHandler: FeedHandler,
) {
  try {
    const subaccountProtos = subaccountIds.map((sa) => {
      const [owner, number] = sa.split("/");
      return { owner, number: parseInt(number, 10) };
    });

    const request: StreamOrderbookUpdatesRequest = {
      clobPairId: clobPairIds,
      subaccountIds: subaccountProtos,
    };

    console.log(request);

    const responses = await client.streamOrderbookUpdates(request);

    console.log(responses);
    for await (const response of responses) {
      console.log(response);
    }

    //   .call.on("data", (response: StreamOrderbookUpdatesResponse) => {
    //     try {
    //       const fillEvents = feedHandler.handle(response);
    //       if (process.env.PRINT_FILLS === "true") {
    //         printFills(fillEvents, cpidToMarketInfo);
    //       }
    //       if (process.env.PRINT_ACCOUNTS === "true") {
    //         printSubaccounts(feedHandler.getRecentSubaccountUpdates());
    //       }
    //     } catch (error) {
    //       throw new Error(`Error handling message: ${JSON.stringify(error)}`);
    //     }
    //   });
    //
    // call.on("error", (error: grpc.ServiceError) => {
    //   throw new Error(`gRPC error occurred: ${error.code} - ${error.details}`);
    // });
    //
    // call.on("end", () => {
    //   console.log("gRPC stream ended");
    // });
  } catch (error) {
    throw new Error(`Unexpected error in gRPC stream: ${error}`);
  }
}

async function listenToWebSocket(
  websocket: WebSocket,
  cpidToMarketInfo: Record<number, any>,
  feedHandler: FeedHandler,
) {
  try {
    websocket.on("message", async (message: WebSocket.Data) => {
      try {
        // Parse the incoming data into a protobuf object
        const response = JSON.parse(
          message.toString(),
        ) as StreamOrderbookUpdatesResponse;

        // Update the order book state and print any fills
        const fillEvents = feedHandler.handle(response);
        if (process.env.PRINT_FILLS === "true") {
          printFills(fillEvents, cpidToMarketInfo);
        }
        if (process.env.PRINT_ACCOUNTS === "true") {
          printSubaccounts(feedHandler.getRecentSubaccountUpdates());
        }
      } catch (error) {
        throw new Error(`Error handling message: ${message.toString()}`);
      }
    });

    websocket.on("close", () => {
      console.log("WebSocket stream ended");
    });

    websocket.on("error", (error: any) => {
      throw new Error(`WebSocket stream error occurred: ${error}`);
    });
  } catch (error) {
    throw new Error(`Unexpected error in WebSocket stream: ${error}`);
  }
}

function printFills(fillEvents: Fill[], cpidToMarketInfo: Record<number, any>) {
  fillEvents.forEach((fill) => {
    const info = cpidToMarketInfo[fill.clobPairId];
    const ar = info.atomicResolution;
    const qce = info.quantumConversionExponent;

    console.log(
      [
        fill.execMode !== 7 ? "(optimistic)" : "(finalized)",
        fill.fillType,
        fill.takerIsBuy ? "buy" : "sell",
        quantumsToSize(fill.quantums.toNumber(), ar),
        "@",
        subticksToPrice(fill.subticks.toNumber(), ar, qce),
        `taker=${fill.taker}`,
        `maker=${fill.maker}`,
      ].join(" "),
    );
  });
}

function printSubaccounts(subaccountsDict: Record<string, StreamSubaccount>) {
  Object.entries(subaccountsDict).forEach(([subaccountId, subaccount]) => {
    const perpetualPositionsStr = Object.entries(subaccount.perpetualPositions)
      .map(
        ([perpId, perpPosition]) =>
          `Perpetual ID: ${perpId}, Quantums: ${perpPosition.quantums}`,
      )
      .join(", ");

    const assetPositionsStr = Object.entries(subaccount.assetPositions)
      .map(
        ([assetId, assetPosition]) =>
          `Asset ID: ${assetId}, Quantums: ${assetPosition.quantums}`,
      )
      .join(", ");

    console.log(
      [
        `Subaccount ID: ${subaccount.subaccountId.ownerAddress}/${subaccount.subaccountId.subaccountNumber}`,
        `Perpetual Positions: ${perpetualPositionsStr}`,
        `Asset Positions: ${assetPositionsStr}`,
      ].join(" | "),
    );
  });
}

async function printBooksEveryNMs(
  feedHandler: FeedHandler,
  cpidToMarketInfo: Record<number, any>,
  ms: number,
) {
  while (true) {
    await new Promise((resolve) => setTimeout(resolve, ms));
    Object.entries(feedHandler.getBooks()).forEach(([clobPairId, book]) => {
      const info = cpidToMarketInfo[parseInt(clobPairId, 10)];
      console.log(`Book for CLOB pair ${clobPairId} (${info.ticker}):`);
      prettyPrintBook(
        book,
        info.atomicResolution,
        info.quantumConversionExponent,
      );
    });
  }
}

function prettyPrintBook(
  book: LimitOrderBook,
  atomicResolution: number,
  quantumConversionExponent: number,
) {
  const topAsks = Array.from(book.asks()).slice(0, 5);
  const topBids = Array.from(book.bids()).slice(0, 5);

  console.log(
    `{"Price":>12} {"Qty":>12} {"Client Id":>12} {"Address":>43} Acc`,
  );

  topAsks.reverse().forEach((o: any) => {
    const price = subticksToPrice(
      o.subticks,
      atomicResolution,
      quantumConversionExponent,
    );
    const size = quantumsToSize(o.quantums, atomicResolution);
    console.log(
      [
        price.toFixed(6),
        size.toFixed(6),
        o.order_id.client_id,
        o.order_id.owner_address,
        o.order_id.subaccount_number,
      ].join(" "),
    );
  });

  console.log(`{"--":>12} {"--":>12}`);

  topBids.forEach((o: any) => {
    const price = subticksToPrice(
      o.subticks,
      atomicResolution,
      quantumConversionExponent,
    );
    const size = quantumsToSize(o.quantums, atomicResolution);
    console.log(
      [
        price.toFixed(6),
        size.toFixed(6),
        o.order_id.client_id,
        o.order_id.owner_address,
        o.order_id.subaccount_number,
      ].join(" "),
    );
  });

  console.log();
}

main().catch((error) => {
  console.error("Error in main:", error);
  if (error instanceof Error) {
    console.error("Stack trace:", error.stack);
  }
});
