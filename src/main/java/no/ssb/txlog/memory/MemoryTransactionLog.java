package no.ssb.txlog.memory;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.txlog.api.TransactionLog;
import no.ssb.txlog.api.TransactionLogEntry;
import no.ssb.txlog.api.TransactionLogReader;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class MemoryTransactionLog implements TransactionLog {

    final ULID ulid;

    final NavigableMap<ULID.Value, TransactionLogEntry> entryByUlid = new ConcurrentSkipListMap<>();

    final AtomicBoolean closed = new AtomicBoolean();

    public MemoryTransactionLog() {
        this.ulid = new ULID();
    }

    @Override
    public void write(TransactionLogEntry entry) {
        if (closed.get()) {
            throw new RuntimeException("Transaction-log is closed");
        }
        TransactionLogEntry previousEntry = entryByUlid.putIfAbsent(entry.transactionId(), entry);
        if (previousEntry != null && previousEntry != entry) {
            throw new IllegalArgumentException("Unable to write entry, another entry with the same transactionId already exists, ulid: " + entry.transactionId().toString());
        }
    }

    @Override
    public TransactionLogReader reader() {
        if (closed.get()) {
            throw new RuntimeException("Transaction-log is closed");
        }
        return new MemoryTransactionLogReader(ulid, new TreeMap<>(entryByUlid));
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        entryByUlid.clear();
        closed.set(true);
    }
}
