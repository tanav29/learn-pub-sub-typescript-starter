import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType } from "../internal/pubsub/subscribe.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  return (ps) => {
    handlePause(gs, ps);
    process.stdout.write("> ");
    return AckType.Ack;
  };
}

export function handlerMove(gs: GameState): (move: ArmyMove) => AckType {
  return (move) => {
    const outcome = handleMove(gs, move);
    process.stdout.write("> ");
    return outcome === MoveOutcome.Safe || outcome === MoveOutcome.MakeWar
      ? AckType.Ack
      : AckType.NackDiscard;
  };
}
