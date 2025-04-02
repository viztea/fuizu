import type { IIdentifyThrottler } from "@discordjs/ws";
import { AsyncEventEmitter } from "@vladfrangu/async_event_emitter";
import { createInbox, JSONCodec, type NatsConnection } from "nats";
import { Pending } from "./pending";

export class DistributedIdentifyThrottler extends AsyncEventEmitter<{ debug: [message: string] }> implements IIdentifyThrottler {
    static JSON = JSONCodec();

    readonly nats: NatsConnection;
    readonly pending: Map<number, Pending> = new Map();
    readonly inbox: string;
    readonly requestSubject: string;

    constructor(nats: NatsConnection, requestSubject: string, inbox = createInbox("shard_queue")) {
        super();

        this.nats = nats;
        this.inbox = inbox;
        this.requestSubject = requestSubject;
    }

    async run() {
        const allowances = this.nats.subscribe(this.inbox);
        for await (const message of allowances) {
            let shardId: number;
            try {
                shardId = message.json<{ id: number }>().id;
            } catch (ex) {
                this.emit("debug", "Failed to deserialize allowance payload");
                continue;
            }

            // 
            const pending = this.pending.get(shardId);
            if (pending) {
                pending.use();
                this.emit("debug", `Received allowance for shard ${shardId}`);
            }

            this.pending.delete(shardId);
        }

        this.emit("debug", "Allowance subscription exited abruptly");
    }

    waitForIdentify(shardId: number, signal: AbortSignal): Promise<void> {
        if (this.pending.has(shardId)) {
            throw new Error("Already requesting identify allowance for shard " + shardId);
        }

        // create new pending promise.
        const pending = new Pending(this, shardId).withSignal(signal);
        this.pending.set(shardId, pending)

        // 
        this.emit("debug", "Requesting allowance for shard " + shardId);
        this.nats.publish(this.requestSubject, DistributedIdentifyThrottler.JSON.encode({ id: shardId }), {
            reply: this.inbox
        });

        pending.request = performance.now();

        return pending.promise;
    }
}
