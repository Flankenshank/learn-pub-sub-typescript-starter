import amqp from "amqplib";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";

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

    const gameState = new GameState(username);

      while (true) {
        const words = await getInput();
        const command = words[0];
        if (command === "spawn") {
          try {
            commandSpawn(gameState, words);
          } catch (err) {
            console.log((err as Error).message);
          }
        } else if (command === "move") {
          try {
            commandMove(gameState, words);
          } catch (err) {
          console.log((err as Error).message);
        }
      } else if (command === "status") {
          commandStatus(gameState);
        } else if (command === "help") {
          printClientHelp();
        } else if (command === "spam") {
          console.log("Spamming not allowed yet!");
        } else if (command === "quit") {
          printQuit();
          break;
        } else {
          console.log(`Unknown command: ${command}`);
        }
    
      }

  }

  main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
  
})