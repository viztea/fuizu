import type { DistributedIdentifyThrottler } from ".";

export class Pending {
    readonly instance: DistributedIdentifyThrottler;
    readonly promise: Promise<void>;
    resolve!: () => void;
    reject!: (error: Error) => void;

    signal: AbortSignal | null = null;
    signalListener: ((event: Event) => void) | null = null;

    request!: number;
    aborted = false;

    constructor(instance: DistributedIdentifyThrottler, readonly shardId: number) {
        this.instance = instance;

        this.promise = new Promise((resolve, reject) => {
            this.resolve = resolve;
            this.reject = reject;
        })
    }

    withSignal(signal: AbortSignal): this {
        if (signal.aborted) {
            this.aborted = true;
            return this;
        }

        this.signal = signal;
        this.signalListener = () => {
            this.instance.pending.delete(this.shardId);
            this.reject(new Error("Request aborted manually"));
        };
        this.signal.addEventListener("abort", this.signalListener);

        return this;
    }

    use() {
        this.dispose();
        this.resolve();
        return this;
    }

    abort() {
        this.dispose();
        this.reject(new Error("Request aborted manually"));
        return this;
    }

    private dispose() {
        if (this.signal) {
            this.signal.removeEventListener("abort", this.signalListener!);
            this.signal = null;
            this.signalListener = null;
        }
    }
}