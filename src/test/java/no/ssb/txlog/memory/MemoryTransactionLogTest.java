package no.ssb.txlog.memory;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.huxhorn.sulky.ulid.ULID;
import no.ssb.txlog.api.TransactionIdGenerator;
import no.ssb.txlog.api.TransactionLogEntry;
import no.ssb.txlog.api.TransactionLogReader;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class MemoryTransactionLogTest {

    final TransactionIdGenerator idGenerator = new TransactionIdGenerator();
    final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void thatWrittenEntryIsAvailableToReader() {
        MemoryTransactionLog txlog = new MemoryTransactionLog();
        ULID.Value txid = idGenerator.nextMonotonic();
        assertFalse(txlog.entryByUlid.containsKey(txid));
        txlog.write(new MemoryTransactionLogEntry(txid, "foo-1.0", "MyEntity", "1", ZonedDateTime.now(), mapper.createObjectNode()));
        assertTrue(txlog.entryByUlid.containsKey(txid));
    }

    @Test
    public void thatReaderCanReadAllElementsFromBeginning() throws InterruptedException {
        MemoryTransactionLog txlog = new MemoryTransactionLog();
        txlog.write(new MemoryTransactionLogEntry(idGenerator.nextMonotonic(), "foo-1.0", "MyEntity", "1", ZonedDateTime.now(), mapper.createObjectNode()));
        txlog.write(new MemoryTransactionLogEntry(idGenerator.nextMonotonic(), "foo-1.0", "MyEntity", "2", ZonedDateTime.now(), mapper.createObjectNode()));
        txlog.write(new MemoryTransactionLogEntry(idGenerator.nextMonotonic(), "foo-1.0", "MyEntity", "3", ZonedDateTime.now(), mapper.createObjectNode()));

        TransactionLogReader reader = txlog.reader();
        TransactionLogEntry entry1 = reader.receive(1, TimeUnit.SECONDS);
        assertEquals(entry1.resourceId(), "1");
        TransactionLogEntry entry2 = reader.receive(1, TimeUnit.SECONDS);
        assertEquals(entry2.resourceId(), "2");
        TransactionLogEntry entry3 = reader.receive(1, TimeUnit.SECONDS);
        assertEquals(entry3.resourceId(), "3");
        assertNull(reader.receive(1, TimeUnit.SECONDS));
    }

    @Test
    public void thatFindUlidWorks() {
        MemoryTransactionLog txlog = new MemoryTransactionLog();
        txlog.write(new MemoryTransactionLogEntry(idGenerator.nextMonotonic(), "foo-1.0", "MyEntity", "1", ZonedDateTime.now(), mapper.createObjectNode()));
        ULID.Value txid2 = idGenerator.nextMonotonic();
        txlog.write(new MemoryTransactionLogEntry(txid2, "foo-1.0", "MyEntity", "2", ZonedDateTime.now(), mapper.createObjectNode()));
        txlog.write(new MemoryTransactionLogEntry(idGenerator.nextMonotonic(), "foo-1.0", "MyEntity", "3", ZonedDateTime.now(), mapper.createObjectNode()));

        // attempt to find second entry

        TransactionLogReader reader = txlog.reader();
        TransactionLogEntry entry = reader.find(txid2, Duration.ofSeconds(30));
        assertEquals(entry.transactionId(), txid2);
        assertEquals(entry.resourceId(), "2");
    }

    @Test
    public void thatSeekWorks() throws InterruptedException {
        MemoryTransactionLog txlog = new MemoryTransactionLog();
        ZonedDateTime now = ZonedDateTime.now();
        ZonedDateTime lowerBound = now.minusMinutes(1);
        ZonedDateTime upperBound = now.plusMinutes(1);
        ZonedDateTime lowerQuarter = now.minusSeconds(30);
        ZonedDateTime upperQuarter = now.plusSeconds(30);
        txlog.write(new MemoryTransactionLogEntry(idGenerator.nextValue(lowerBound), "foo-1.0", "MyEntity", "lowerBound", lowerBound, mapper.createObjectNode()));
        txlog.write(new MemoryTransactionLogEntry(idGenerator.nextValue(lowerQuarter), "foo-1.0", "MyEntity", "lowerQuarter", lowerQuarter, mapper.createObjectNode()));
        txlog.write(new MemoryTransactionLogEntry(idGenerator.nextValue(now), "foo-1.0", "MyEntity", "now", now, mapper.createObjectNode()));
        txlog.write(new MemoryTransactionLogEntry(idGenerator.nextValue(upperQuarter), "foo-1.0", "MyEntity", "upperQuarter", upperQuarter, mapper.createObjectNode()));
        txlog.write(new MemoryTransactionLogEntry(idGenerator.nextValue(upperBound), "foo-1.0", "MyEntity", "upperBound", upperBound, mapper.createObjectNode()));

        TransactionLogReader reader = txlog.reader();

        // seek to 1 second before lowerBound
        reader.seek(lowerBound.minusSeconds(1));
        assertEquals(reader.receive(1, TimeUnit.SECONDS).resourceId(), "lowerBound");

        // seek to 1 second before lowerQuarter
        reader.seek(lowerQuarter.minusSeconds(1));
        assertEquals(reader.receive(1, TimeUnit.SECONDS).resourceId(), "lowerQuarter");

        {
            /*
             * Do more rigorous testing right around now timestamp as given by transaction-id
             */

            // seek to 1 second before now
            reader.seek(now.minusSeconds(1));
            assertEquals(reader.receive(1, TimeUnit.SECONDS).resourceId(), "now");

            // seek to 1 millisecond before now
            reader.seek(now.minus(1, ChronoUnit.MILLIS));
            assertEquals(reader.receive(1, TimeUnit.SECONDS).resourceId(), "now");

            // seek to now
            reader.seek(now);
            assertEquals(reader.receive(1, TimeUnit.SECONDS).resourceId(), "now");

            // seek to 1 millisecond after now
            reader.seek(now.plus(1, ChronoUnit.MILLIS));
            assertEquals(reader.receive(1, TimeUnit.SECONDS).resourceId(), "upperQuarter");

            // seek to 1 second after now
            reader.seek(now.plusSeconds(1));
            assertEquals(reader.receive(1, TimeUnit.SECONDS).resourceId(), "upperQuarter");

        }

        // seek to 1 second before upperQuarter
        reader.seek(upperQuarter.minusSeconds(1));
        assertEquals(reader.receive(1, TimeUnit.SECONDS).resourceId(), "upperQuarter");

        // seek to 1 second before upperBound
        reader.seek(upperBound.minusSeconds(1));
        assertEquals(reader.receive(1, TimeUnit.SECONDS).resourceId(), "upperBound");

        // seek to 1 second after upperBound
        reader.seek(upperBound.plusSeconds(1));
        assertNull(reader.receive(1, TimeUnit.SECONDS));
    }
}
