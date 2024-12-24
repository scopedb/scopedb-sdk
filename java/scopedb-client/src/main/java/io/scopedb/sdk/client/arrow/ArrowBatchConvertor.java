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
