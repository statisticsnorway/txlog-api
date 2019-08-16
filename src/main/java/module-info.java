import no.ssb.txlog.api.TransactionLog;
import no.ssb.txlog.memory.MemoryTransactionLog;

module no.ssb.txlog.api {
    requires transitive de.huxhorn.sulky.ulid;
    requires transitive com.fasterxml.jackson.databind;

    exports no.ssb.txlog.api;

    provides TransactionLog with MemoryTransactionLog;
}
