package no.ssb.txlog.memory;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.txlog.api.TransactionIdGenerator;
import org.testng.annotations.Test;

import java.time.ZonedDateTime;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class MemoryTransactionLogTest {

    @Test
    public void thatWrittenEntryIsAvailableToReader() {
        TransactionIdGenerator idGenerator = new TransactionIdGenerator();
        MemoryTransactionLog txlog = new MemoryTransactionLog();
        ULID.Value txid = idGenerator.nextMonotonic();
        assertFalse(txlog.entryByUlid.containsKey(txid));
        txlog.write(new MemoryTransactionLogEntry(txid, "foo-1.0", "MyEntity", "1", ZonedDateTime.now(), null));
        assertTrue(txlog.entryByUlid.containsKey(txid));
    }
}
