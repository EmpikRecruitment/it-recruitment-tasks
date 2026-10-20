import {
  ORDERS_DLQ_TOPIC,
  ORDERS_TOPIC,
  Order,
  disconnectProducer,
  getProducer,
  sendOrder,
  sendRaw,
  waitForMessage,
} from "../src/kafkaHelpers";

describe("obsługa błędów w DLQ:", () => {
  test("niepoprawny JSON trafia do kolejki DLQ", async () => {
    const producer = await getProducer();
    const brokenJSON = '{"broken":';
    const receivedPromise = waitForMessage(
      ORDERS_DLQ_TOPIC,
      (msg) => typeof msg.error === "string" && msg.error.includes("Niepoprawny JSON")
    );

    await sendRaw(producer, brokenJSON, "bad-json", ORDERS_TOPIC);

    const receivedMessage = await receivedPromise;

    expect(receivedMessage).toEqual(
      expect.objectContaining({
        error: expect.stringContaining("Niepoprawny JSON"),
      })
    );
  });

  test("sprawdza zwrotkę przy brakujcych polach", async () => {
    const producer = await getProducer();
    const invalidOrder: Partial<Order> = {
      orderId: "to nie tak powinno wygldać",
      customer: "niedobry customer",
    };
    const receivedPromise = waitForMessage(
      ORDERS_DLQ_TOPIC,
      (msg) => typeof msg.error === "string" && msg.error.includes("Brakujące pola")
    );

    await sendOrder(producer, invalidOrder);

    const receivedMessage = await receivedPromise;

    expect(receivedMessage).toEqual(
      expect.objectContaining({
        error: expect.stringContaining("Brakujące pola"),
      })
    );
  });

  afterAll(async () => {
    await disconnectProducer();
  });
});
