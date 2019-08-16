package no.ssb.txlog.tck;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.huxhorn.sulky.ulid.ULID;
import no.ssb.service.provider.api.ProviderConfigurator;
import no.ssb.txlog.api.TransactionIdGenerator;
import no.ssb.txlog.api.TransactionLog;
import no.ssb.txlog.api.TransactionLogEntry;
import no.ssb.txlog.api.TransactionLogInitializer;
import no.ssb.txlog.api.TransactionLogReader;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TransactionLogTck {

    final TransactionIdGenerator idGenerator = new TransactionIdGenerator();

    @Test
    public void thatAllEntryDataIsRetainedInTransactionLog() throws InterruptedException {
        TransactionLog txlog = ProviderConfigurator.configure(Map.of(), "memory", TransactionLogInitializer.class);
        ObjectMapper mapper = new ObjectMapper();
        ULID.Value transactionId = idGenerator.nextMonotonic();
        ZonedDateTime timestamp = ZonedDateTime.now();
        ObjectNode data = mapper.createObjectNode();
        txlog.write(txlog.builder().transactionId(transactionId).schema("foo-1.0").domain("MyEntity").resourceId("1").timestamp(timestamp).data(data).build());

        TransactionLogReader reader = txlog.reader();
        TransactionLogEntry retained = reader.receive(1, TimeUnit.SECONDS);
        assertEquals(retained.transactionId(), transactionId);
        assertEquals(retained.schema(), "foo-1.0");
        assertEquals(retained.domain(), "MyEntity");
        assertEquals(retained.resourceId(), "1");
        assertEquals(retained.timestamp().toInstant().atZone(ZoneId.of("Etc/UTC")), timestamp.toInstant().atZone(ZoneId.of("Etc/UTC")));
        assertEquals(retained.data(), mapper.createObjectNode());
    }

    @Test
    public void thatReaderCanReadAllElementsFromBeginning() throws InterruptedException {
        TransactionLog txlog = ProviderConfigurator.configure(Map.of(), "memory", TransactionLogInitializer.class);
        txlog.write(txlog.builder().transactionId(idGenerator.nextMonotonic()).schema("foo-1.0").domain("MyEntity").resourceId("1").timestamp(ZonedDateTime.now()).build());
        txlog.write(txlog.builder().transactionId(idGenerator.nextMonotonic()).schema("foo-1.0").domain("MyEntity").resourceId("2").timestamp(ZonedDateTime.now()).build());
        txlog.write(txlog.builder().transactionId(idGenerator.nextMonotonic()).schema("foo-1.0").domain("MyEntity").resourceId("3").timestamp(ZonedDateTime.now()).build());

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
        TransactionLog txlog = ProviderConfigurator.configure(Map.of(), "memory", TransactionLogInitializer.class);
        ULID.Value txid2 = idGenerator.nextMonotonic();
        txlog.write(txlog.builder().transactionId(idGenerator.nextMonotonic()).schema("foo-1.0").domain("MyEntity").resourceId("1").timestamp(ZonedDateTime.now()).build());
        txlog.write(txlog.builder().transactionId(txid2).schema("foo-1.0").domain("MyEntity").resourceId("2").timestamp(ZonedDateTime.now()).build());
        txlog.write(txlog.builder().transactionId(idGenerator.nextMonotonic()).schema("foo-1.0").domain("MyEntity").resourceId("3").timestamp(ZonedDateTime.now()).build());

        // attempt to find second entry

        TransactionLogReader reader = txlog.reader();
        TransactionLogEntry entry = reader.find(txid2, Duration.ofSeconds(30));
        assertEquals(entry.transactionId(), txid2);
        assertEquals(entry.resourceId(), "2");
    }

    @Test
    public void thatSeekWorks() throws InterruptedException {
        TransactionLog txlog = ProviderConfigurator.configure(Map.of(), "memory", TransactionLogInitializer.class);
        ZonedDateTime now = ZonedDateTime.now();
        ZonedDateTime lowerBound = now.minusMinutes(1);
        ZonedDateTime upperBound = now.plusMinutes(1);
        ZonedDateTime lowerQuarter = now.minusSeconds(30);
        ZonedDateTime upperQuarter = now.plusSeconds(30);
        txlog.write(txlog.builder().transactionId(idGenerator.nextValue(lowerBound)).schema("foo-1.0").domain("MyEntity").resourceId("lowerBound").timestamp(ZonedDateTime.now()).build());
        txlog.write(txlog.builder().transactionId(idGenerator.nextValue(lowerQuarter)).schema("foo-1.0").domain("MyEntity").resourceId("lowerQuarter").timestamp(ZonedDateTime.now()).build());
        txlog.write(txlog.builder().transactionId(idGenerator.nextValue(now)).schema("foo-1.0").domain("MyEntity").resourceId("now").timestamp(ZonedDateTime.now()).build());
        txlog.write(txlog.builder().transactionId(idGenerator.nextValue(upperQuarter)).schema("foo-1.0").domain("MyEntity").resourceId("upperQuarter").timestamp(ZonedDateTime.now()).build());
        txlog.write(txlog.builder().transactionId(idGenerator.nextValue(upperBound)).schema("foo-1.0").domain("MyEntity").resourceId("upperBound").timestamp(ZonedDateTime.now()).build());

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
