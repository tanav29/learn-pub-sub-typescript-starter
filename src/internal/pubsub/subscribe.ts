import type amqp from "amqplib";
import { declareAndBind, SimpleQueueType } from "./consume.js";

export enum AckType {
  Ack,
  NackRequeue,
  NackDiscard,
}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => AckType,
): Promise<void> {
  const [channel, queue] = await declareAndBind(conn, exchange, queueName, key, queueType);
  await channel.consume(queue.queue, (message: amqp.ConsumeMessage | null) => {
    if (message === null) return;
    const data: T = JSON.parse(message.content.toString());
    switch (handler(data)) {
      case AckType.Ack:
        channel.ack(message);
        console.log("Ack");
        break;
      case AckType.NackRequeue:
        channel.nack(message, false, true);
        console.log("NackRequeue");
        break;
      case AckType.NackDiscard:
        channel.nack(message, false, false);
        console.log("NackDiscard");
        break;
    }
  });
}
