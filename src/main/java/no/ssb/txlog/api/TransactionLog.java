package no.ssb.txlog.api;

public interface TransactionLog extends AutoCloseable {

    void write(TransactionLogEntry entry);

    TransactionLogReader reader();

    TransactionLogEntry.Builder builder();

    boolean isClosed();

}
