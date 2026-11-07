import amqp from "amqplib";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connection was succesful...");
  console.log("Starting Peril server...");

  process.on('SIGINT', () => {
    conn.close();
    console.log("Closed connection with rabbit mq...");
    console.log("Stopping the server...")
    process.exit(0);
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
