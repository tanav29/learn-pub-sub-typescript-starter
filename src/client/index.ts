import amqp from "amqplib";
import { clientWelcome, printClientHelp } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connection was succesful...");
  console.log("Starting Peril client...");

  const username = await clientWelcome();
  await declareAndBind(conn, ExchangePerilDirect, `${PauseKey}.${username}`, PauseKey, SimpleQueueType.Transient);

  process.on('SIGINT', () => {
    conn.close();
    console.log("Closed connection with rabbit mq...");
    console.log("Stopping the client...")
    process.exit(0);
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
