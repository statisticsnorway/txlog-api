package no.ssb.txlog.api;

import de.huxhorn.sulky.ulid.ULID;

import java.time.ZonedDateTime;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Guarantees lexicographically strictly increasing ulid values.
 */
public class TransactionIdGenerator {
    final ULID ulid = new ULID();
    final AtomicReference<ULID.Value> previousValue = new AtomicReference<>();

    public TransactionIdGenerator() {
        previousValue.set(ulid.nextValue());
    }

    public ULID getUlid() {
        return ulid;
    }

    public ULID.Value nextMonotonic() {
        ULID.Value previousUlid;
        ULID.Value value;
        do {
            previousUlid = previousValue.get();
            value = ulid.nextStrictlyMonotonicValue(previousUlid).orElseThrow();
        } while (!previousValue.compareAndSet(previousUlid, value));
        return value;
    }

    public ULID.Value nextValue(ZonedDateTime timestamp) {
        return ulid.nextValue(Date.from(timestamp.toInstant()).getTime());
    }
}
