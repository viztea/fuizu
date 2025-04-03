import type { IIdentifyThrottler } from "@discordjs/ws";
import type { APIGatewayBotInfo } from "discord-api-types/payloads/v10";
import { AsyncEventEmitter } from "@vladfrangu/async_event_emitter";
import { createInbox, JSONCodec, Payload, type NatsConnection } from "nats";
import { Pending } from "./pending";
import { hostname } from "os";

export interface Options {
    fuizuSubject: string;
    inbox?: string;
    hostName?: string;
}

type makeFuizuRequest<T extends string, D> = { type: T, data: D }

export type FuizuRequest =
    | makeFuizuRequest<"retrieve_gateway", undefined>
    | makeFuizuRequest<"identify", { id: number, host_name: string }>



function encodeRequest<T extends FuizuRequest["type"]>(type: T, data: Extract<FuizuRequest, { type: T }>["data"]): Payload {
    return DistributedIdentifyThrottler.JSON.encode({ type, data });
}

export class DistributedIdentifyThrottler extends AsyncEventEmitter<{ debug: [message: string] }> implements IIdentifyThrottler {
    static JSON = JSONCodec();

    readonly nats: NatsConnection;
    readonly pending: Map<number, Pending> = new Map();
    readonly options: Required<Options>;

    #running = false;

    constructor(nats: NatsConnection, options: Options) {
        super();

        this.nats = nats;
        this.options = {
            hostName: hostname(),
            inbox: createInbox("shard_queue"),
            ...options
        }
    }

    get running() {
        return this.#running;
    }

    /**
     * Subscribe to the inbox for identify allowance messages.
     * 
     * This should be ran in parallel with the rest of the application, as it will block until the subscription is closed;
     * as such, this function exiting should be treated as a fatal error since new IDENTIFY requests will not be fulfilled.
     */
    async run() {
        this.#running = true;

        const allowances = this.nats.subscribe(this.options.inbox);
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
        this.#running = false;
    }

    /**
     * Fetch the gateway information that the Fuizu server is currently using.
     * 
     * This should be used over manually fetching the gateway information from Discord, as it will
     * prevent your Bot from being rate limited.
     * 
     * @returns Gateway information
     */
    async fetchGatewayInformation(): Promise<APIGatewayBotInfo> {
        const response = await this.nats.request(
            this.options.fuizuSubject,
            encodeRequest("retrieve_gateway", undefined)
        );

        return response.json();
    }

    waitForIdentify(shardId: number, signal: AbortSignal): Promise<void> {
        if (!this.running) {
            throw new Error("Throttler is not running");
        }

        if (this.pending.has(shardId)) {
            throw new Error("Already requesting identify allowance for shard " + shardId);
        }

        // create new pending promise.
        const pending = new Pending(this, shardId).withSignal(signal);
        this.pending.set(shardId, pending)

        // 
        this.emit("debug", "Requesting allowance for shard " + shardId);
        this.nats.publish(
            this.options.fuizuSubject,
            encodeRequest("identify", { id: shardId, host_name: this.options.hostName }),
            { reply: this.options.inbox }
        );

        pending.request = performance.now();

        return pending.promise;
    }
}
