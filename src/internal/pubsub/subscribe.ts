import type amqp from "amqplib";
import { declareAndBind, SimpleQueueType } from "./consume.js";

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => void,
): Promise<void> {
  const [channel, queue] = await declareAndBind(conn, exchange, queueName, key, queueType);
  await channel.consume(queue.queue, (message: amqp.ConsumeMessage | null) => {
    if (message === null) return;
    const data: T = JSON.parse(message.content.toString());
    handler(data);
    channel.ack(message);
  });
}
