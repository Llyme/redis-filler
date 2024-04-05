import { Redis } from "ioredis";

export class RedisFiller {
    /**
     * @type {Object.<string, *[]>}
     */
    #fillers = {};
    /**
     * @type {Redis}
     */
    #redis = null;

    /**
     * 
     * @param {Redis} redis
     */
    constructor(redis) {
        this.#redis = redis;
    }

    /**
     * Clears all fillers.
     */
    clear() {
        this.#fillers = {};
    }

    /**
     * @param {string} key
     * @param {...*} values The values that will be added.
     * @returns 
     */
    push(key, ...values) {
        if (!(key in this.#fillers))
            this.#fillers[key] = [];

        this.#fillers[key].push(...values);
    }

    /**
     * Pops redis fillers.
     * 
     * Nothing happens if there are no fillers for the given key.
     * @param {string} key 
     * @param {number} count 
     * @returns 
     */
    pop(key, count = 1) {
        if (!(key in this.#fillers))
            return;

        for (; count > 0 && this.#fillers[key].length > 0; count--)
            this.#fillers[key].pop();

        if (this.#fillers[key].length == 0)
            delete this.#fillers[key];
    }

    filter(key, predicate) {
        if (!(key in this.#fillers))
            return;

        this.#fillers[key] = this.#fillers[key].filter(predicate);

        if (this.#fillers[key].length == 0)
            delete this.#fillers[key];
    }

    /**
     * Publishes the fillers to redis via `Set`.
     * 
     * @param {(key: string, value: any) => string} valueSelector 
     */
    async publish(valueSelector = undefined) {
        const pipeline = this.#redis.pipeline();

        for (const { key, filler } of this.enumerateFillers())
            for (const value of filler)
                pipeline.sadd(
                    key,
                    valueSelector != null
                        ? valueSelector(key, value)
                        : value.toString()
                );

        return await pipeline.exec();
    }

    *enumerateFillers() {
        for (const key in this.#fillers) {
            const filler = this.#fillers[key];

            if (filler.length == 0)
                continue;

            yield {
                key,
                filler
            };
        }
    }
}
