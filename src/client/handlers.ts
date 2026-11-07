import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import type { ConfirmChannel } from "amqplib";
import type { ArmyMove, RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType } from "../internal/pubsub/subscribe.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";
import { ExchangePerilTopic, WarRecognitionsPrefix } from "../internal/routing/routing.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  return (ps) => {
    handlePause(gs, ps);
    process.stdout.write("> ");
    return AckType.Ack;
  };
}

export function handlerMove(gs: GameState, channel: ConfirmChannel): (move: ArmyMove) => Promise<AckType> {
  return async (move) => {
    const outcome = handleMove(gs, move);
    if (outcome === MoveOutcome.MakeWar) {
      const rw: RecognitionOfWar = { attacker: move.player, defender: gs.getPlayerSnap() };
      try {
        await publishJSON(channel, ExchangePerilTopic, `${WarRecognitionsPrefix}.${gs.getUsername()}`, rw);
      } catch (error) {
        console.error("Failed to publish war declaration:", error);
        process.stdout.write("> ");
        return AckType.NackRequeue;
      }
    }
    process.stdout.write("> ");
    return outcome === MoveOutcome.Safe || outcome === MoveOutcome.MakeWar
      ? AckType.Ack
      : AckType.NackDiscard;
  };
}

export function handlerWar(gs: GameState): (war: RecognitionOfWar) => Promise<AckType> {
  return async (war) => {
    const outcome = handleWar(gs, war).result;
    process.stdout.write("> ");
    switch (outcome) {
      case WarOutcome.NotInvolved: return AckType.NackRequeue;
      case WarOutcome.NoUnits: return AckType.NackDiscard;
      case WarOutcome.OpponentWon:
      case WarOutcome.YouWon:
      case WarOutcome.Draw: return AckType.Ack;
      default:
        console.error("Unexpected war outcome:", outcome);
        return AckType.NackDiscard;
    }
  };
}
