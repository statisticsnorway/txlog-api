package no.ssb.txlog.api;

import de.huxhorn.sulky.ulid.ULID;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;

public interface TransactionLogReader extends AutoCloseable {

    /**
     * Get the next entry in the transaction-log. If no entry is available right away, this method will block waiting
     * for at most timeout units to get the next entry, or return null indicating the timeout.
     *
     * @param timeout
     * @param unit
     * @return
     * @throws InterruptedException if the calling thread is interrupted while waiting for a transaction-log entry
     */
    TransactionLogEntry receive(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Position
     *
     * @param id        the transaction-id of the entry to look for
     * @param tolerance the maximum allowed clock-drift + processing-delay (from client ULID generation to server
     *                  places the entry in the transaction-log sequence)
     * @return the entry matching the given id if found in transaction log within the range:
     * [ timestamp(id) - tolerance, timestamp(id) + tolerance ], otherwise null.
     */
    TransactionLogEntry find(ULID.Value id, Duration tolerance);

    /**
     * Position the stream at the given timestamp. This should approximately match the timestamps of the generated ULID
     * values offset by clock-drift and processing delay from ULID value generation to transaction-log write sequencing.
     *
     * @param timestamp
     */
    void seek(ZonedDateTime timestamp);

    /**
     * Returns whether or not the transaction-log-reader is closed.
     *
     * @return whether the reader is closed
     */
    boolean isClosed();
}
