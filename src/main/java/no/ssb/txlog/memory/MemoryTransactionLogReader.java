package no.ssb.txlog.memory;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.txlog.api.TransactionLogEntry;
import no.ssb.txlog.api.TransactionLogReader;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class MemoryTransactionLogReader implements TransactionLogReader {

    final ULID ulid;

    final NavigableMap<ULID.Value, TransactionLogEntry> entryByUlid;

    final AtomicReference<Iterator<TransactionLogEntry>> iterator = new AtomicReference<>();

    final AtomicBoolean closed = new AtomicBoolean();

    MemoryTransactionLogReader(ULID ulid, NavigableMap<ULID.Value, TransactionLogEntry> entryByUlid) {
        this.ulid = ulid;
        this.entryByUlid = entryByUlid;
        this.iterator.set(entryByUlid.values().iterator());
    }

    @Override
    public TransactionLogEntry receive(long timeout, TimeUnit unit) {
        if (!iterator.get().hasNext()) {
            return null;
        }
        return iterator.get().next();
    }

    @Override
    public TransactionLogEntry find(ULID.Value id, Duration tolerance) {
        return entryByUlid.get(id);
    }

    @Override
    public void seek(ZonedDateTime timestamp) {
        ULID.Value ulidValue = ulid.nextValue(Date.from(timestamp.toInstant()).getTime());
        ULID.Value lowerBound = new ULID.Value(ulidValue.getMostSignificantBits() & 0xFFFFFFFFFFFF0000L, 0L); // first value with timestamp
        iterator.set(entryByUlid.tailMap(lowerBound).values().iterator());
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        closed.set(true);
    }
}
