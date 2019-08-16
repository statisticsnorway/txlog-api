package no.ssb.txlog.api;

import com.fasterxml.jackson.databind.JsonNode;
import de.huxhorn.sulky.ulid.ULID;

import java.time.ZonedDateTime;

public interface TransactionLogEntry {

    ULID.Value transactionId();

    String schema();

    String domain();

    String resourceId();

    ZonedDateTime timestamp();

    JsonNode data();
}
