import { connect } from "nats";
import { DistributedIdentifyThrottler } from ".";

(async () => {
    const ctl = new AbortController();

    let lastGlobalIdentify = performance.now();
    let queues = new Map<number, number>();

    let total = 0;

    const launch = async (shard: number) => {
        const queueKey = shard % 16;
        if (queueKey == 0) {
            console.log("-".repeat(10))
        }

        const lastQueueIdentify = queues.get(queueKey) ?? performance.now();

        await throttler.waitForIdentify(shard, ctl.signal);

        // 
        const now = performance.now();
        console.log(
            (total++).toString().padStart(4, '0'), "/",
            shard.toString().padStart(4, '0'), "=>",
            queueKey.toString().padStart(2, '0'),
            "GI:", ((now - lastGlobalIdentify) / 1000).toFixed(2),
            "|",
            "QI:", ((now - lastQueueIdentify) / 1000).toFixed(2),
        );

        // update timings
        queues.set(queueKey, now);
        lastGlobalIdentify = now;
    }

    const throttler = new DistributedIdentifyThrottler(await connect({ servers: ["chanel:4222"] }), { fuizuSubject: "fuizu.identifies" });

    throttler.run();

    Array.from({ length: 32 }, (_, i) => i).forEach((shard) => launch(shard));
})();