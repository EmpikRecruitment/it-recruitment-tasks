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
    const receivedPromise = waitForMessage(ORDERS_PROCESSED_TOPIC, (msg) => msg.orderId === order.orderId);

    await sendOrder(producer, order);

    const received = await receivedPromise;

    expect(received).toEqual(
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
