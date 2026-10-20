import { randomUUID } from "crypto";
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
    const requestId = randomUUID();
    const receivedPromise = waitForMessage(
      ORDERS_DLQ_TOPIC,
      (msg, metadata) => metadata?.key === requestId && !!msg && typeof msg === "object"
    );

    await sendRaw(producer, brokenJSON, requestId, ORDERS_TOPIC);

    const receivedMessage = await receivedPromise;

    expect(receivedMessage).toEqual(
      expect.objectContaining({
        error: expect.stringContaining("Niepoprawny JSON"),
      })
    );
  });

  test("sprawdza zwrotkę przy brakujcych polach", async () => {
    const producer = await getProducer();
    const requestId = randomUUID();
    const invalidOrder: Partial<Order> = {
      orderId: "to nie tak powinno wygldać",
      customer: "niedobry customer",
    };
    const receivedPromise = waitForMessage(
      ORDERS_DLQ_TOPIC,
      (msg, metadata) => metadata?.key === requestId && !!msg && typeof msg === "object"
    );

    await sendOrder(producer, invalidOrder, "orders", requestId);

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
