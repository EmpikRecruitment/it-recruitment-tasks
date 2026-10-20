import { randomUUID } from "crypto";
import {
  ORDERS_PROCESSED_TOPIC,
  Order,
  disconnectProducer,
  getProducer,
  makeTestOrder,
  sendOrder,
  waitForMessage,
} from "../src/kafkaHelpers";

describe("poprawne nowe zamówienie:", () => {
  test("order ze statusem NEW trafia do PROCESSED", async () => {
    const producer = await getProducer();
    const order: Order = makeTestOrder({ status: "NEW" });
    const requestId = randomUUID();
    const receivedPromise = waitForMessage(
      ORDERS_PROCESSED_TOPIC,
      (msg, metadata) => metadata?.key === requestId && msg.orderId === order.orderId
    );

    await sendOrder(producer, order, "orders", requestId);

    const receivedMessage = await receivedPromise;

    expect(receivedMessage).toEqual(
      expect.objectContaining({
        orderId: order.orderId,
        customer: order.customer,
        amount: order.amount,
        status: "PROCESSED",
      })
    );
  });

  afterAll(async () => {
    await disconnectProducer();
  });
});
