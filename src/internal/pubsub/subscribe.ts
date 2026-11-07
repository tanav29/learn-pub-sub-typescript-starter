import type { ChannelModel } from "amqplib";
import { subscribe, SimpleQueueType, type AckType } from "./consume.js";

export { AckType } from "./consume.js";

export function subscribeJSON<T>(
  conn: ChannelModel, exchange: string, queueName: string, key: string,
  queueType: SimpleQueueType, handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  return subscribe(conn, exchange, queueName, key, queueType, handler, (data) => JSON.parse(data.toString()));
}
