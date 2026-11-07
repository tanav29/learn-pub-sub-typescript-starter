import amqp from "amqplib";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  const channel = await conn.createConfirmChannel();
  const state: PlayingState = { isPaused: true };
  await publishJSON(channel, ExchangePerilDirect, PauseKey, state);
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

