/**
 * TWOJE ZADANIE: zaimplementuj testy.
 *
 * Aplikacja (producer + consumer) jest już gotowa i działająca — Twoim
 * zadaniem jest napisanie testów E2E weryfikujących jej zachowanie.
 *
 * Masz do dyspozycji gotowe helpery z `src/kafkaHelpers.ts` (opisane
 * w komentarzach tego pliku) — możesz z nich korzystać, ale nie musisz:
 * jeśli uważasz, że lepiej napisać coś inaczej, zrób to i uzasadnij w README.
 *
 */
import { Producer } from "kafkajs";
import {
  ORDERS_DLQ_TOPIC,
  ORDERS_PROCESSED_TOPIC,
  ORDERS_TOPIC,
  Order,
  collectMessages,
  disconnectProducer,
  getProducer,
  makeTestOrder,
  sendOrder,
  sendRaw,
  waitForMessage,
} from "../src/kafkaHelpers";

describe("Przepływ przetwarzania zamówień przez Kafkę", () => {
  let producer: Producer;

  beforeAll(async () => {
    producer = await getProducer();
    console.log("Producer connected XXX");
  });

  test("Zamówienie ze statusem NEW jest przetworzone i wysłane do ORDERS_PROCESSED_TOPIC", async () => {
    const order: Order = makeTestOrder({ status: "NEW" });
    const receivedPromise = waitForMessage(ORDERS_PROCESSED_TOPIC, (msg) => msg.orderId === order.orderId);

    await sendOrder(producer, order);

    const received = await receivedPromise;

    expect(received).toEqual(
      expect.objectContaining({
        orderId: order.orderId,
        status: "PROCESSED",
      })
    );
  });

  afterAll(async () => {
    await disconnectProducer();
  });

});
