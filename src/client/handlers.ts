import { handlePause } from "../internal/gamelogic/pause.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import type { ackType } from "../internal/pubsub/consume.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => ackType {
    return (ps: PlayingState) => {
        handlePause(gs, ps);
        process.stdout.write("> ")
        return "Ack";
    };
}

export function handlerMove(gs: GameState): (move: ArmyMove) => ackType {
    return (move: ArmyMove) => {
        handleMove(gs, move);
        process.stdout.write("> ")
        if (MoveOutcome.Safe === handleMove(gs, move) || MoveOutcome.MakeWar === handleMove(gs, move)) {
            return "Ack";
        }
        return "NackDiscard";
    };
}