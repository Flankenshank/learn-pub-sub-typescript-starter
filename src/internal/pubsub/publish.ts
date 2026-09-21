import { type ConfirmChannel } from "amqplib";
import { encode } from "@msgpack/msgpack";

export function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  const jsonString = JSON.stringify(value);
  const buffer = Buffer.from(jsonString, "utf-8");
  return new Promise((resolve, reject) => {
    ch.publish(exchange, routingKey, buffer, {contentType: "application/json"}, (err, ok) => {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
};

export function publishMsgPack<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  const buffer = Buffer.from(encode(value));
  return new Promise((resolve, reject) => {
    ch.publish(
      exchange, 
      routingKey, 
      buffer, 
      {contentType: "application/x-msgpack"},
      (err) => {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
};