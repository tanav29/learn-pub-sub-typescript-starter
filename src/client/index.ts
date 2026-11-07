import amqp from "amqplib";
import { clientWelcome, closeInput, commandStatus, getInput, printClientHelp, printQuit } from "../internal/gamelogic/gamelogic.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connection was succesful...");
  console.log("Starting Peril client...");

  const username = await clientWelcome();
  await declareAndBind(conn, ExchangePerilDirect, `${PauseKey}.${username}`, PauseKey, SimpleQueueType.Transient);
  const gameState = new GameState(username);

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
            else commandMove(gameState, words);
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
        case "spam":
          console.log("Spamming not allowed yet!");
          break;
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
