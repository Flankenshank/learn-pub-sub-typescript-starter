import amqp from "amqplib";

async function main() {
  const rabbitConnString  = await amqp.connect("amqp://guest:guest@localhost:5672");
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connected to RabbitMQ");
  process.on("SIGINT", async () => {
    console.log("\nShutting down...");
    await conn.close();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
