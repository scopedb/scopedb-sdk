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
import io.scopedb.sdk.client.request.IngestResponse;
import io.scopedb.sdk.client.request.ResultFormat;
import io.scopedb.sdk.client.request.StatementRequest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

class ScopeDBClientTest {
    @Test
    public void testReadAfterWrite() throws Exception {
        final ScopeDBConfig config =
                ScopeDBConfig.builder().endpoint("http://localhost:6543").build();


        final ScopeDBClientNG client = new ScopeDBClientNG(config);

        final List<AutoCloseable> allocated = new ArrayList<>();
        try {
            final BufferAllocator allocator = new RootAllocator();
            allocated.add(allocator);

            System.out.println("Creating table...");
            final StatementRequest createTableRequest = StatementRequest.builder()
                    .statement("CREATE TABLE IF NOT EXISTS t(i INT)")
                    .format(ResultFormat.ArrowJson)
                    .build();
            process(client, createTableRequest, allocator);

            System.out.println("Ingest Data...");
            final List<VectorSchemaRoot> batches = makeBatches(allocator);
            allocated.addAll(batches);
            final IngestResponse ingestResponse =
                    client.ingestArrowBatch(batches, "INSERT INTO t").join();
            System.out.println("Ingested: " + ingestResponse);

            System.out.println("Query Data...");
            final StatementRequest readTableRequest = StatementRequest.builder()
                    .statement("FROM t")
                    .format(ResultFormat.ArrowJson)
                    .build();
            process(client, readTableRequest, allocator);
        } finally {
            Collections.reverse(allocated);
            AutoCloseables.close(allocated);
        }
    }

    private static List<VectorSchemaRoot> makeBatches(BufferAllocator allocator) {
        final Field field = new Field("i", FieldType.nullable(new ArrowType.Int(64, true)), null);
        final Schema schema = new Schema(Collections.singletonList(field));

        final VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        root.allocateNew();
        final BigIntVector v = (BigIntVector) root.getVector(0);
        v.allocateNew(3);
        v.set(0, 42);
        v.setNull(1);
        v.set(2, -21);
        root.setRowCount(3);
        return Collections.singletonList(root);
    }

    private static void process(ScopeDBClientNG client, StatementRequest request, BufferAllocator allocator) {
        final List<VectorSchemaRoot> batches = client.submit(request, true)
                .thenApply(r -> {
                    final String rows = r.getResultSet().getRows();
                    return ArrowBatchConvertor.readArrowBatch(rows, allocator);
                })
                .join();
        for (VectorSchemaRoot batch : batches) {
            System.out.println(batch.contentToTSVString());
            batch.close();
        }
    }
}
