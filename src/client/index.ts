import amqp from "amqplib";
import type { ConfirmChannel } from "amqplib";
import type { GameLog } from "../internal/gamelogic/logs.js";
import { clientWelcome, closeInput, commandStatus, getInput, getMaliciousLog, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { SimpleQueueType } from "../internal/pubsub/consume.js";
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js";
import { subscribeJSON } from "../internal/pubsub/subscribe.js";
import { ArmyMovesPrefix, ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import { handlerMove, handlerPause, handlerWar } from "./handlers.js";

export function publishGameLog(channel: ConfirmChannel, username: string, message: string): Promise<void> {
  const log: GameLog = { username, message, currentTime: new Date() };
  return publishMsgPack(channel, ExchangePerilTopic, `${GameLogSlug}.${username}`, log);
}

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connection was succesful...");
  console.log("Starting Peril client...");

  const username = await clientWelcome();
  const gameState = new GameState(username);
  const publishChannel = await conn.createConfirmChannel();
  await subscribeJSON(conn, ExchangePerilDirect, `${PauseKey}.${username}`, PauseKey, SimpleQueueType.Transient, handlerPause(gameState));
  const moveKey = `${ArmyMovesPrefix}.${username}`;
  await subscribeJSON(conn, ExchangePerilTopic, moveKey, `${ArmyMovesPrefix}.*`, SimpleQueueType.Transient, handlerMove(gameState, publishChannel));
  await subscribeJSON(conn, ExchangePerilTopic, WarRecognitionsPrefix, `${WarRecognitionsPrefix}.*`, SimpleQueueType.Durable, handlerWar(gameState, (username, message) => publishGameLog(publishChannel, username, message)));

  process.on('SIGINT', () => {
    conn.close();
    console.log("Closed connection with rabbit mq...");
    console.log("Stopping the client...")
    process.exit(0);
  });

  try {
    while (true) {
      const words = await getInput();
      switch (words[0]) {
        case "spawn":
        case "move":
          try {
            if (words[0] === "spawn") commandSpawn(gameState, words);
            else {
              const move = commandMove(gameState, words);
              await publishJSON(publishChannel, ExchangePerilTopic, moveKey, move);
              console.log("Move published successfully.");
            }
          } catch (err) {
            if (err instanceof Error) console.error(err.message);
            else throw err;
          }
          break;
        case "status":
          await commandStatus(gameState);
          break;
        case "help":
          printClientHelp();
          break;
        case "spam": {
          const countText = words[1];
          const count = Number(countText);
          if (!countText || !/^\d+$/.test(countText) || !Number.isSafeInteger(count) || count < 1) {
            console.error("Usage: spam <positive integer>");
            break;
          }
          try {
            for (let i = 0; i < count; i++) {
              await publishGameLog(publishChannel, username, getMaliciousLog());
            }
            console.log(`Published ${count} game logs.`);
          } catch (error) {
            console.error("Failed to publish game log:", error);
          }
          break;
        }
        case "quit":
          printQuit();
          return;
        default:
          console.log("Didn't understand!");
      }
    }
  } finally {
    closeInput();
    await conn.close();
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
