package no.ssb.txlog.memory;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.txlog.api.TransactionIdGenerator;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TransactionIdGeneratorTest {

    @Test
    public void thatTimestampInTransactionIdIsCorrectlyPreserved() {
        /*
         * ULID will only store 48 bits, which means millisecond precision. ZonedDateTime will often have higher
         * precision than that, so ensure that we generate an instance that only has millisecond precision.
         */
        TransactionIdGenerator idGenerator = new TransactionIdGenerator();
        ZonedDateTime now = Instant.ofEpochMilli(ZonedDateTime.now().toInstant().toEpochMilli()).atZone(ZoneId.of("Etc/UTC"));
        ULID.Value value = idGenerator.nextValue(now);
        ZonedDateTime preserved = new Date(value.timestamp()).toInstant().atZone(ZoneId.of("Etc/UTC"));
        assertEquals(preserved, now);
    }

    @Test
    public void thatIdsAreMonotonicallyIncreasing() {
        TransactionIdGenerator idGenerator = new TransactionIdGenerator();
        ULID.Value previousValue = idGenerator.nextMonotonic();
        for (int i = 0; i < 10000; i++) {
            ULID.Value value = idGenerator.nextMonotonic();
            assertTrue(value.compareTo(previousValue) > 0);
        }
    }
}
