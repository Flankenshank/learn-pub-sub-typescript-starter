import amqp, { type Channel } from "amqplib";

export enum SimpleQueueType {
  Durable,
  Transient,
}

export enum AckType {
  Ack = "Ack",
  NackRequeue = "NackRequeue",
  NackDiscard = "NackDiscard",
}

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
  handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
  await declareAndBind(conn, exchange, queueName, key, queueType)
    .then(([channel, queue]) => {
      console.log(`Subscribed to queue ${queue.queue} with key ${key}`);
      channel.consume(queue.queue, async (msg: amqp.ConsumeMessage | null) => {
        if (msg === null) {
          return;
        } else {
          const data = JSON.parse(msg.content.toString()) as T;
          const ack = await handler(data);
          switch (ack) {
            case AckType.Ack:
              channel.ack(msg);
              console.log (`Acknowledged message from queue ${queue.queue}`);
              break;
            case AckType.NackRequeue:
              channel.nack(msg, false, true);
              console.log (`Nacked message from queue ${queue.queue} with requeue`);
              break;
            case AckType.NackDiscard:
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