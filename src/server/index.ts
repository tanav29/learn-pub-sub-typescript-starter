import amqp from "amqplib";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { AckType, subscribeMsgPack, SimpleQueueType } from "../internal/pubsub/consume.js";
import { writeLog, type GameLog } from "../internal/gamelogic/logs.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey } from "../internal/routing/routing.js";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  await subscribeMsgPack<GameLog>(conn, ExchangePerilTopic, GameLogSlug, `${GameLogSlug}.*`, SimpleQueueType.Durable, async (log) => {
    try {
      await writeLog(log);
      return AckType.Ack;
    } catch (error) {
      console.error("Failed to write game log:", error);
      return AckType.NackRequeue;
    } finally {
      process.stdout.write("> ");
    }
  });
  const channel = await conn.createConfirmChannel();
  const state: PlayingState = { isPaused: true };
  await publishJSON(channel, ExchangePerilDirect, PauseKey, state);
  console.log("Connection was succesful...");
  console.log("Starting Peril server...");

  // Used to run the server from a non-interactive source, like the multiserver.sh file
  if (!process.stdin.isTTY) {
    console.log("Non-interactive mode: skipping command input.");
    return;
  }

  printServerHelp();

  loop: while (true) {
    const words = await getInput();
    if (words.length == 0) continue;

    switch (words[0]) {
      case "pause":
        console.log("Sending a pause message!")
        await publishJSON(channel, ExchangePerilDirect, PauseKey, {
          isPaused: true
        } as PlayingState)
        break;
      case "resume":
        console.log("Sending a resume message!")
        await publishJSON(channel, ExchangePerilDirect, PauseKey, {
          isPaused: false
        } as PlayingState)
        break;
      case "quit":
        break loop;
      default:
        console.log("Didn't understand!")
        break;
    }
  }

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
