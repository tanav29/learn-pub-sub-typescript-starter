import type { Channel, ChannelModel, Replies } from "amqplib";
import { decode } from "@msgpack/msgpack";
import { ExchangePerilDeadLetter } from "../routing/routing.js";

export enum SimpleQueueType {
  Durable,
  Transient,
}

export enum AckType {
  Ack,
  NackRequeue,
  NackDiscard,
}

export async function subscribe<T>(
  conn: ChannelModel, exchange: string, queueName: string, routingKey: string,
  simpleQueueType: SimpleQueueType, handler: (data: T) => Promise<AckType> | AckType,
  deserializer: (data: Buffer) => T,
): Promise<void> {
  const [channel, queue] = await declareAndBind(conn, exchange, queueName, routingKey, simpleQueueType);
  await channel.prefetch(10);
  await channel.consume(queue.queue, async (message) => {
    if (message === null) return;
    try {
      const result = await handler(deserializer(message.content));
      switch (result) {
        case AckType.Ack: channel.ack(message); break;
        case AckType.NackRequeue: channel.nack(message, false, true); break;
        case AckType.NackDiscard: channel.nack(message, false, false); break;
      }
    } catch (error) {
      console.error("Failed to process message:", error);
      channel.nack(message, false, true);
    }
  });
}

export function subscribeMsgPack<T>(
  conn: ChannelModel, exchange: string, queueName: string, routingKey: string,
  queueType: SimpleQueueType, handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  return subscribe(conn, exchange, queueName, routingKey, queueType, handler, (data) => decode(data) as T);
}

export async function declareAndBind(
  conn: ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, Replies.AssertQueue]> {
  const channel = await conn.createChannel();
  const queue = await channel.assertQueue(queueName, {
    durable: queueType === SimpleQueueType.Durable,
    autoDelete: queueType === SimpleQueueType.Transient,
    exclusive: queueType === SimpleQueueType.Transient,
    arguments: { "x-dead-letter-exchange": ExchangePerilDeadLetter },
  });
  await channel.bindQueue(queue.queue, exchange, key);
  return [channel, queue];
}
