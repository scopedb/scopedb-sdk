package io.scopedb.sdk.client;

import io.scopedb.sdk.client.request.ResultFormat;
import io.scopedb.sdk.client.request.StatementRequest;
import java.util.List;
import org.junit.jupiter.api.Test;

class ScopeDBClientTest {
    @Test
    public void testReadAfterWrite() {
        final ScopeDBConfig config =
                ScopeDBConfig.builder().endpoint("http://localhost:6543").build();
        final ScopeDBClient client = new ScopeDBClient(config);
        final StatementRequest request = StatementRequest.builder()
                .statement("FROM test")
                .format(ResultFormat.ArrowJson)
                .waitTimeout("60s")
                .build();
        final List<String> batches = client.queryAsArrowBatch(request).join();
        System.out.println(batches);
    }
}
