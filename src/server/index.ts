import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey } from "../internal/routing/routing.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";

async function main() {
  const rabbitConn  = await amqp.connect("amqp://guest:guest@localhost:5672");
  const channel = await rabbitConn.createConfirmChannel();
  console.log("Connected to RabbitMQ");
  printServerHelp();

  const gameLogQueue = await declareAndBind(rabbitConn, ExchangePerilDirect , GameLogSlug, "game_logs.*", SimpleQueueType.Durable);

  while (true) {
    const words = await getInput();
    const command = words[0];
    if (command === "pause") {
      console.log("Sending a pause message.");
      await publishJSON(channel, ExchangePerilDirect , PauseKey, { isPaused: true });
    } else if (command === "resume") {
      console.log("Sending a resume message.");
      await publishJSON(channel, ExchangePerilDirect , PauseKey, { isPaused: false });
    } else if (command === "quit") {
      console.log("Exiting...");
      break;
    } else {
      console.log(`Unknown command: ${command}`);
    }

  }

  process.on("SIGINT", async () => {
    console.log("\nShutting down...");
    await rabbitConn.close();
    process.exit(0);
  });


publishJSON(channel, ExchangePerilDirect , PauseKey, { isPaused: true })
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