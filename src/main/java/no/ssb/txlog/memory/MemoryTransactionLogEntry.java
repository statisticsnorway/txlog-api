package no.ssb.txlog.memory;

import com.fasterxml.jackson.databind.JsonNode;
import de.huxhorn.sulky.ulid.ULID;
import no.ssb.txlog.api.TransactionLogEntry;

import java.time.ZonedDateTime;

public class MemoryTransactionLogEntry implements TransactionLogEntry {
    final ULID.Value transactionId;
    final String schema;
    final String domain;
    final String resourceId;
    final ZonedDateTime timestamp;
    final JsonNode data;

    public MemoryTransactionLogEntry(ULID.Value transactionId, String schema, String domain, String resourceId, ZonedDateTime timestamp, JsonNode data) {
        this.transactionId = transactionId;
        this.schema = schema;
        this.domain = domain;
        this.resourceId = resourceId;
        this.timestamp = timestamp;
        this.data = data;
    }

    @Override
    public ULID.Value transactionId() {
        return transactionId;
    }

    @Override
    public String schema() {
        return schema;
    }

    @Override
    public String domain() {
        return domain;
    }

    @Override
    public String resourceId() {
        return resourceId;
    }

    @Override
    public ZonedDateTime timestamp() {
        return timestamp;
    }

    @Override
    public JsonNode data() {
        return data;
    }
}
