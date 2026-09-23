import { handlePause } from "../internal/gamelogic/pause.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import type { ArmyMove, RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import { AckType } from "../internal/pubsub/consume.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilTopic, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import type { ConfirmChannel } from "amqplib";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";
import { channel } from "diagnostics_channel";
import { publishGameLog } from "./index.js"



export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
    return (ps: PlayingState) => {
        handlePause(gs, ps);
        process.stdout.write("> ")
        return AckType.Ack;
    };
}

export function handlerMove(gs: GameState, ch: ConfirmChannel): (move: ArmyMove) => Promise<AckType> {
    return async (move: ArmyMove) => {
        try {
        const outcome = handleMove(gs, move);
        if (MoveOutcome.Safe === outcome) {
            return AckType.Ack;
        } else if (MoveOutcome.MakeWar === outcome) {
            try {
            const rw: RecognitionOfWar = {
                attacker: move.player,
                defender: gs.getPlayerSnap(),
            };
            await publishJSON(ch, ExchangePerilTopic, `${WarRecognitionsPrefix}.${gs.getUsername()}`, rw);
            return AckType.Ack;
        } catch (err) {
            console.error("Error publishing war recognition:", err);
            return AckType.NackRequeue;
        }}
        return AckType.NackDiscard;
    } catch (err) {
        console.error("Error handling move:", err);
        return AckType.NackDiscard;
    } finally {
        process.stdout.write("> ");
    }}}

export function handlerWar(gs: GameState, ch: ConfirmChannel): (rw: RecognitionOfWar) => Promise<AckType> {
    return async (rw: RecognitionOfWar) => {
        try {
        const outcome = handleWar(gs, rw);
        switch (outcome.result) {
            case WarOutcome.NotInvolved:
                return AckType.NackDiscard;
            case WarOutcome.NoUnits:
                return AckType.NackDiscard;
            case WarOutcome.YouWon:
                try {
                    console.log("Attempting to publish game log...");
                    await publishGameLog(ch, gs.getUsername(), `${outcome.winner} won a war against ${outcome.loser}`)
                    return AckType.Ack;
                } catch (err) {
                    console.log("Failed to publish game log:", err);
                    return AckType.NackRequeue;
                }
            case WarOutcome.OpponentWon:
                try {
                    console.log("Attempting to publish game log...");
                    await publishGameLog(ch, gs.getUsername(), `${outcome.winner} won a war against ${outcome.loser}`);
                    console.log("Game log published successfully!");
                    return AckType.Ack;
                } catch (err) {
                    console.log("Failed to publish game log:", err);
                    return AckType.NackRequeue;
                }
            case WarOutcome.Draw:
                try {
                    console.log("Attempting to publish game log...");
                    await publishGameLog(ch, gs.getUsername(),`A war between ${outcome.attacker} and ${outcome.defender} resulted in a draw`)
                    return AckType.Ack;
                } catch (err) {
                    console.log("Failed to publish game log:", err);
                    return AckType.NackRequeue;
                }
            default:
                console.error("Error handling war recognition:", outcome);
                return AckType.NackDiscard;
        }
    } catch (err) {
        console.error("Error handling war recognition:", gs, rw, err);
        return AckType.NackDiscard;
    } finally {
        process.stdout.write("> ");
    }}}