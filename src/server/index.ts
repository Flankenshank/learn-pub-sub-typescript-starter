import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
  const rabbitConn  = await amqp.connect("amqp://guest:guest@localhost:5672");
  console.log("Connected to RabbitMQ");

  const channel = await rabbitConn.createConfirmChannel();

  process.on("SIGINT", async () => {
    console.log("\nShutting down...");
    await rabbitConn.close();
    process.exit(0);
  });


publishJSON(channel, ExchangePerilDirect, PauseKey, { isPaused: true })
  .then(() => {
    console.log("Message published successfully");
  })
  .catch((err) => {
    console.error("Failed to publish message:", err);
  });

}

  main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
  
});