import amqp, { type Channel } from "amqplib";

export enum SimpleQueueType {
  Durable,
  Transient,
}

export type ackType = "Ack" | "NackRequeue" | "NackDiscard";

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
    const channel = await conn.createChannel();

    const queue = await channel.assertQueue(queueName, {
      durable: queueType === SimpleQueueType.Durable,
      autoDelete: queueType === SimpleQueueType.Transient,
      exclusive: queueType === SimpleQueueType.Transient,
      arguments: {
        "x-dead-letter-exchange": "peril_dlx",
      },
    });
    
    await channel.bindQueue(queue.queue, exchange, key);
    return [channel, queue];
};

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => ackType,
): Promise<void> {
  await declareAndBind(conn, exchange, queueName, key, queueType)
    .then(([channel, queue]) => {
      console.log(`Subscribed to queue ${queue.queue} with key ${key}`);
      channel.consume(queue.queue, (msg: amqp.ConsumeMessage | null) => {
        if (msg === null) {
          return;
        } else {
          const data = JSON.parse(msg.content.toString()) as T;
          const ack = handler(data);
          switch (ack) {
            case "Ack":
              channel.ack(msg);
              console.log (`Acknowledged message from queue ${queue.queue}`);
              break;
            case "NackRequeue":
              channel.nack(msg, false, true);
              console.log (`Nacked message from queue ${queue.queue} with requeue`);
              break;
            case "NackDiscard":
              channel.nack(msg, false, false);
              console.log (`Nacked message from queue ${queue.queue} with discard`);
              break;
          }
        }
      });
    })
    .catch((err) => {
      console.error("Failed to subscribe:", err);
    });
}