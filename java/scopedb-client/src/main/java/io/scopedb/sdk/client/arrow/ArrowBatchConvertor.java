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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.table.Table;

public final class ArrowBatchConvertor {
    /**
     * Read arrow batch from BASE64 encoded rows string.
     *
     * @param rows      BASE64 encoded rows string
     * @param allocator BufferAllocator to allocate memory
     * @return Arrow batches as a list of {@link VectorSchemaRoot}
     */
    public List<VectorSchemaRoot> readArrowBatches(String rows, BufferAllocator allocator) {
        final List<VectorSchemaRoot> batches = new ArrayList<>();
        final byte[] data = Base64.getDecoder().decode(rows);
        final ByteArrayInputStream stream = new ByteArrayInputStream(data);
        try (final ArrowStreamReader reader = new ArrowStreamReader(stream, allocator)) {
            while (reader.loadNextBatch()) {
                batches.add(new Table(reader.getVectorSchemaRoot()).toVectorSchemaRoot());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return batches;
    }
}
