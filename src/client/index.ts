import amqp from "amqplib";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import { clientWelcome } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";

async function main() {
    const rabbitConn  = await amqp.connect("amqp://guest:guest@localhost:5672");
    console.log("Connected to RabbitMQ");
  
    process.on("SIGINT", async () => {
      console.log("\nShutting down...");
      await rabbitConn.close();
      process.exit(0);
    });

    const username = await clientWelcome();
    console.log(`Username set to: ${username}`);
    const [, queue] = await declareAndBind(rabbitConn, ExchangePerilDirect, `pause.${username}`, PauseKey, SimpleQueueType.Transient);
    console.log(`Queue ${queue.queue} declared and bound to exchange ${ExchangePerilDirect} with key ${PauseKey}`);

  }

  main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
  
})