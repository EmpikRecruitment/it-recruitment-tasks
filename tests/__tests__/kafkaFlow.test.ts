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
import {
  ORDERS_PROCESSED_TOPIC,
  Order,
  disconnectProducer,
  getProducer,
  makeTestOrder,
  sendOrder,
  waitForMessage,
} from "../src/kafkaHelpers";

describe("Przepływ przetwarzania zamówień przez Kafkę", () => {
  const producerPromise = getProducer();

  test("Zamówienie ze statusem NEW jest przetworzone i wysłane do topicu orders-processed", async () => {
    const producer = await producerPromise;
    const order: Order = makeTestOrder({ status: "NEW" });
    const receivedPromise = waitForMessage(ORDERS_PROCESSED_TOPIC, (msg) => msg.orderId === order.orderId);

    await sendOrder(producer, order);

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
