package no.ssb.txlog.memory;

import no.ssb.service.provider.api.ProviderName;
import no.ssb.txlog.api.TransactionLog;
import no.ssb.txlog.api.TransactionLogInitializer;

import java.util.Map;
import java.util.Set;

@ProviderName("memory")
public class MemoryTransactionLogInitializer implements TransactionLogInitializer {

    @Override
    public String providerId() {
        return "memory";
    }

    @Override
    public Set<String> configurationKeys() {
        return Set.of();
    }

    @Override
    public TransactionLog initialize(Map<String, String> configuration) {
        return new MemoryTransactionLog();
    }
}
