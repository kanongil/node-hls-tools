
/**
 * Generic timestamp representation using an arbitrary base.
 */
export class TimeStamp {

    value;
    base;

    constructor(value, base) {

        this.value = BigInt(value);
        this.base = BigInt(base);
    }

    add(ts) {

        if (this.base === ts.base || ts.value === 0n) {
            return new TimeStamp(this.value + ts.value, this.base);
        }

        let base = this.base * ts.base;
        let value = this.value * ts.base + ts.value * this.base;

        if (value === 0n) {
            base = this.base;
        }
        else if (value % ts.base === 0n) {
            value /= ts.base;
            base = this.base;
        }
        else if (value % this.base === 0n) {
            value /= this.base;
            base = ts.base;
        }

        return new TimeStamp(value, base);
    }

    subtract(ts) {

        return this.add(new TimeStamp(-ts.value, ts.base));
    }

    greaterThan(ts) {

        if (this.base === ts.base) {
            return this.value > ts.value;
        }

        const res = this.subtract(ts);
        return res.value > 0;
    }

    /** Time in ms */
    valueOf() {

        return Number(this.value * 1000n) / Number(this.base);
    }
}
