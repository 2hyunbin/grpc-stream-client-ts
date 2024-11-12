import { Long } from "@grpc/proto-loader";
import { maxBy, minBy } from "lodash";

export type OrderId = {
  ownerAddress: string;
  subaccountNumber: number;
  clientId: number;
  orderFlags: number;
};

export class Order {
  constructor(
    public orderId: OrderId,
    public isBid: boolean,
    public originalQuantums: Long,
    public quantums: Long,
    public subticks: Long,
  ) {}
}

class ListNode {
  public prev: ListNode | null = null;
  public next: ListNode | null = null;

  constructor(public data: Order) {}
}

class DoublyLinkedList {
  public head: ListNode | null = null;
  public tail: ListNode | null = null;

  append(data: Order): ListNode {
    const newNode = new ListNode(data);
    if (!this.head) {
      this.head = this.tail = newNode;
    } else {
      this.tail!.next = newNode;
      newNode.prev = this.tail;
      this.tail = newNode;
    }
    return newNode;
  }

  remove(nodeToRemove: ListNode): void {
    if (nodeToRemove.prev) {
      nodeToRemove.prev.next = nodeToRemove.next;
    } else {
      this.head = nodeToRemove.next;
    }
    if (nodeToRemove.next) {
      nodeToRemove.next.prev = nodeToRemove.prev;
    } else {
      this.tail = nodeToRemove.prev;
    }
    nodeToRemove.prev = nodeToRemove.next = null;
  }

  isEqual(other: DoublyLinkedList): boolean {
    const myList = Array.from(this);
    const otherList = Array.from(other);
    if (myList.length !== otherList.length) return false;

    for (let i = 0; i < myList.length; i++) {
      if (myList[i] !== otherList[i]) return false;
    }
    return true;
  }

  *[Symbol.iterator](): IterableIterator<Order> {
    let current = this.head;
    while (current) {
      yield current.data;
      current = current.next;
    }
  }
}

export class LimitOrderBook {
  private oidToOrderNode: Map<OrderId, ListNode> = new Map();
  private _asks: Map<Long, DoublyLinkedList> = new Map();
  private _bids: Map<Long, DoublyLinkedList> = new Map();

  private static getOrCreateLevel(
    subticks: Long,
    bookSide: Map<Long, DoublyLinkedList>,
  ): DoublyLinkedList {
    if (!bookSide.has(subticks)) {
      bookSide.set(subticks, new DoublyLinkedList());
    }
    return bookSide.get(subticks)!;
  }

  addOrder(order: Order): Order {
    const bookSide = order.isBid ? this._bids : this._asks;
    const level = LimitOrderBook.getOrCreateLevel(order.subticks, bookSide);
    const orderNode = level.append(order);
    this.oidToOrderNode.set(order.orderId, orderNode);
    return order;
  }

  removeOrder(oid: OrderId): Order | null {
    const orderNode = this.oidToOrderNode.get(oid);
    if (!orderNode) return null;

    const order = orderNode.data;
    const bookSide = order.isBid ? this._bids : this._asks;
    const level = bookSide.get(order.subticks)!;

    level.remove(orderNode);
    if (!level.head) bookSide.delete(order.subticks);

    this.oidToOrderNode.delete(oid);
    return order;
  }

  getOrder(oid: OrderId): Order | null {
    const node = this.oidToOrderNode.get(oid);
    return node ? node.data : null;
  }

  *asks(): IterableIterator<Order> {
    for (const [price, level] of Array.from(this._asks.entries()).sort(
      ([a], [b]) => a.subtract(b).toNumber(),
    )) {
      yield* level;
    }
  }

  *bids(): IterableIterator<Order> {
    for (const [price, level] of Array.from(this._bids.entries()).sort(
      ([a], [b]) => b.subtract(a).toNumber(),
    )) {
      yield* level;
    }
  }

  getMidpointPrice(): number | null {
    const highestBid = maxBy(Array.from(this._bids.keys()), (value) =>
      value.toNumber(),
    );
    const lowestAsk = minBy(Array.from(this._asks.keys()), (value) =>
      value.toNumber(),
    );

    if (!highestBid || !lowestAsk) {
      throw new Error(
        "No highest Bid or lowest Ask found in getMidpointPrice.",
      );
    }

    if (
      Number.isSafeInteger(highestBid.toNumber()) ||
      Number.isSafeInteger(lowestAsk.toNumber())
    )
      return null;
    return highestBid.add(lowestAsk).toNumber() / 2.0;
  }

  compareBooks(other: LimitOrderBook): boolean {
    const numOrders = this.oidToOrderNode.size;
    const numAsks = this._asks.size;
    const numBids = this._bids.size;
    const midpointPrice = this.getMidpointPrice();

    const otherNumOrders = other.oidToOrderNode.size;
    const otherNumAsks = other._asks.size;
    const otherNumBids = other._bids.size;
    const otherMidpointPrice = other.getMidpointPrice();

    if (
      numOrders !== otherNumOrders ||
      numAsks !== otherNumAsks ||
      numBids !== otherNumBids ||
      midpointPrice !== otherMidpointPrice
    ) {
      return false;
    }

    for (const [price, dll] of this._asks) {
      const otherDll = other._asks.get(price);
      if (!otherDll || !dll.isEqual(otherDll)) return false;
    }

    for (const [price, dll] of this._bids) {
      const otherDll = other._bids.get(price);
      if (!otherDll || !dll.isEqual(otherDll)) return false;
    }

    return true;
  }
}

function asksBidsFromBook(book: LimitOrderBook): [Order[], Order[]] {
  return [Array.from(book.asks()), Array.from(book.bids())];
}

function assertBooksEqual(book1: LimitOrderBook, book2: LimitOrderBook): void {
  const [feedAsks, feedBids] = asksBidsFromBook(book1);
  const [snapAsks, snapBids] = asksBidsFromBook(book2);

  if (JSON.stringify(feedAsks) !== JSON.stringify(snapAsks)) {
    debugBookSide(feedAsks, snapAsks);
    throw new Error("Asks for book do not match");
  }
  if (JSON.stringify(feedBids) !== JSON.stringify(snapBids)) {
    debugBookSide(feedBids, snapBids);
    throw new Error("Bids for book do not match");
  }
}

function debugBookSide(haveSide: Order[], expectSide: Order[]): void {
  console.log(`   ${"have".padStart(38)} | ${"expect".padStart(38)}`);
  console.log(
    `🟠 ${"px".padStart(12)} ${"sz".padStart(12)} ${"cid".padStart(12)} | ${"px".padStart(12)} ${"sz".padStart(12)} ${"cid".padStart(12)}`,
  );

  for (let i = 0; i < Math.max(haveSide.length, expectSide.length); i++) {
    const have = haveSide[i];
    const expect = expectSide[i];
    const status =
      have && expect && JSON.stringify(have) === JSON.stringify(expect)
        ? "🟢"
        : "🔴";
    console.log(
      `${status} ${have?.subticks?.toString().padStart(12) || ""} ${have?.quantums?.toString().padStart(12) || ""} ${have?.orderId.clientId?.toString().padStart(12) || ""} | ${expect?.subticks?.toString().padStart(12) || ""} ${expect?.quantums?.toString().padStart(12) || ""} ${expect?.orderId.clientId?.toString().padStart(12) || ""}`,
    );
  }
}
