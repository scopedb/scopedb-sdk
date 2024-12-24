/*
 * Copyright 2024 ScopeDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.scopedb.sdk.client;

import io.scopedb.sdk.client.arrow.ArrowBatchConvertor;
import io.scopedb.sdk.client.request.ResultFormat;
import io.scopedb.sdk.client.request.StatementRequest;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
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

        try (final BufferAllocator allocator = new RootAllocator()) {
            final ArrowBatchConvertor convertor = new ArrowBatchConvertor();
            final List<VectorSchemaRoot> batches = client.execute(request).thenApply(r -> {
                final String rows = r.getResultSet().getRows();
                return convertor.readArrowBatches(rows, allocator);
            }).join();
            for (VectorSchemaRoot batch : batches) {
                System.out.println(batch.contentToTSVString());
                batch.close();
            }
        }
    }
}
