package io.scopedb.sdk.client;

import com.google.gson.Gson;
import io.scopedb.sdk.client.exception.ScopeDBException;
import io.scopedb.sdk.client.request.FetchStatementParams;
import io.scopedb.sdk.client.request.StatementRequest;
import io.scopedb.sdk.client.request.StatementResponse;
import io.scopedb.sdk.client.request.StatementStatus;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.jetbrains.annotations.NotNull;

public class ScopeDBClient {
    private static final MediaType JSON = MediaType.get("application/json");
    private static final Gson GSON = new Gson();

    private final ScopeDBConfig config;
    private final OkHttpClient client;

    public ScopeDBClient(ScopeDBConfig config) {
        this.config = config;
        this.client = new OkHttpClient.Builder().build();
    }

    public CompletableFuture<Void> execute(StatementRequest request) {
        return this.query(request).thenApply(r -> null);
    }

    public CompletableFuture<List<String>> queryAsArrowBatch(StatementRequest request) {
        // TODO(tisonkun): perhaps execute decode in a dedicated thread pool
        return this.query(request).thenApply(r -> {
            final String rows = r.getResultSet().getRows();
            final byte[] data = Base64.getDecoder().decode(rows);
            final List<String> batches = new ArrayList<>();
            // TODO(tisonkun): figure out whether a global allocator helps
            try (final BufferAllocator rootAllocator = new RootAllocator()) {
                try (final ArrowStreamReader reader =
                        new ArrowStreamReader(new ByteArrayInputStream(data), rootAllocator)) {
                    while (reader.loadNextBatch()) {
                        // TODO(tisonkun): how to unload a VectorSchemaRoot reasonably?
                        batches.add(reader.getVectorSchemaRoot().contentToTSVString());
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return batches;
        });
    }

    private CompletableFuture<StatementResponse> query(StatementRequest request) {
        final CompletableFuture<StatementResponse> f = new CompletableFuture<>();

        final HttpUrl url = HttpUrl.Companion.get(config.getEndpoint())
                .newBuilder()
                .addPathSegments("v1/statements")
                .build();
        final RequestBody body = RequestBody.create(GSON.toJson(request), JSON);
        final Request req = new Request.Builder().url(url).post(body).build();

        client.newCall(req).enqueue(new Callback() {
            @Override
            public void onFailure(@NotNull Call call, @NotNull IOException e) {
                f.completeExceptionally(e);
            }

            @Override
            public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
                final ResponseBody body = Objects.requireNonNull(response.body());
                if (response.isSuccessful()) {
                    final StatementResponse resp = GSON.fromJson(body.string(), StatementResponse.class);
                    if (resp.getStatus() != StatementStatus.Finished) {
                        final String statementId = resp.getStatementId();
                        final FetchStatementParams params = FetchStatementParams.builder()
                                .statementId(statementId)
                                .format(request.getFormat())
                                .build();
                        final CompletableFuture<StatementResponse> fut = new CompletableFuture<>();
                        fut.whenComplete((r, e) -> {
                            if (e != null) {
                                f.completeExceptionally(e);
                            } else {
                                f.complete(r);
                            }
                        });
                        ScopeDBClient.this.fetchWithUtilDone(fut, params);
                    } else {
                        f.complete(resp);
                    }
                } else {
                    final ScopeDBException e = ScopeDBException.fromResponse(response.code(), body.string());
                    f.completeExceptionally(e);
                }
            }
        });

        return f;
    }

    private void fetchWithUtilDone(CompletableFuture<StatementResponse> f, FetchStatementParams params) {
        final HttpUrl url = HttpUrl.Companion.get(config.getEndpoint())
                .newBuilder()
                .addPathSegments("v1/statements")
                .addPathSegment(params.getStatementId())
                .addQueryParameter("format", params.getFormat().toParam())
                .build();
        final Request req = new Request.Builder().url(url).build();

        client.newCall(req).enqueue(new Callback() {
            @Override
            public void onFailure(@NotNull Call call, @NotNull IOException e) {
                f.completeExceptionally(e);
            }

            @Override
            public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
                final ResponseBody body = Objects.requireNonNull(response.body());
                if (response.isSuccessful()) {
                    final StatementResponse resp = GSON.fromJson(body.string(), StatementResponse.class);
                    if (resp.getStatus() != StatementStatus.Finished) {
                        ScopeDBClient.this.fetchWithUtilDone(f, params);
                    } else {
                        f.complete(resp);
                    }
                } else {
                    final ScopeDBException e = ScopeDBException.fromResponse(response.code(), body.string());
                    f.completeExceptionally(e);
                }
            }
        });
    }
}
