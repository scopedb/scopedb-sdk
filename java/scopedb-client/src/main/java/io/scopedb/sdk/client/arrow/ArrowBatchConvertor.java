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

package io.scopedb.sdk.client.arrow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

public final class ArrowBatchConvertor {
    /**
     * Read arrow batches from BASE64 encoded rows string.
     *
     * @param rows      BASE64 encoded rows string
     * @param allocator BufferAllocator to allocate memory
     * @return Arrow batches as a list of {@link VectorSchemaRoot}
     */
    @SneakyThrows // IOException
    public static List<VectorSchemaRoot> readArrowBatch(String rows, BufferAllocator allocator) {
        final List<VectorSchemaRoot> batches = new ArrayList<>();
        final byte[] data = Base64.getDecoder().decode(rows);

        @Cleanup ByteArrayInputStream stream = new ByteArrayInputStream(data);
        @Cleanup ArrowStreamReader reader = new ArrowStreamReader(stream, allocator);
        while (reader.loadNextBatch()) {
            final VectorSchemaRoot source = reader.getVectorSchemaRoot();
            final VectorSchemaRoot copy = VectorSchemaRoot.create(source.getSchema(), allocator);
            @Cleanup ArrowRecordBatch recordBatch = new VectorUnloader(source).getRecordBatch();
            new VectorLoader(copy).load(recordBatch);
            batches.add(copy);
        }
        return batches;
    }

    /**
     * Write arrow batches to BASE64 encoded rows string.
     *
     * @param batches Arrow batches as a list of {@link VectorSchemaRoot}
     * @return BASE64 encoded rows string
     */
    @SneakyThrows // IOException
    public static String writeArrowBatch(List<VectorSchemaRoot> batches) {
        @Cleanup ByteArrayOutputStream stream = new ByteArrayOutputStream();
        for (VectorSchemaRoot batch : batches) {
            @Cleanup ArrowStreamWriter writer = new ArrowStreamWriter(batch, null, stream);
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        return Base64.getEncoder().encodeToString(stream.toByteArray());
    }
}
