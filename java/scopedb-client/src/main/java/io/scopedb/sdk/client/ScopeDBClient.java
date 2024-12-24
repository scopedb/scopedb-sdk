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

import com.google.gson.Gson;
import io.scopedb.sdk.client.arrow.ArrowBatchConvertor;
import io.scopedb.sdk.client.exception.ScopeDBException;
import io.scopedb.sdk.client.request.FetchStatementParams;
import io.scopedb.sdk.client.request.IngestData;
import io.scopedb.sdk.client.request.IngestRequest;
import io.scopedb.sdk.client.request.IngestResponse;
import io.scopedb.sdk.client.request.StatementRequest;
import io.scopedb.sdk.client.request.StatementResponse;
import io.scopedb.sdk.client.request.StatementStatus;
import java.io.IOException;
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
import org.apache.arrow.vector.VectorSchemaRoot;
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

    public CompletableFuture<StatementResponse> submit(StatementRequest request) {
        return execute(request, true);
    }

    public CompletableFuture<StatementResponse> execute(StatementRequest request) {
        return execute(request, false);
    }

    public CompletableFuture<StatementResponse> fetch(FetchStatementParams params, boolean untilDone) {
        final CompletableFuture<StatementResponse> f = new CompletableFuture<>();
        fetchWith(f, params, !untilDone);
        return f;
    }

    public CompletableFuture<IngestResponse> ingestArrowBatch(List<VectorSchemaRoot> batches, String statement) {
        final CompletableFuture<IngestResponse> f = new CompletableFuture<>();

        final Request req = makeIngestRequest(ArrowBatchConvertor.writeArrowBatch(batches), statement);
        client.newCall(req).enqueue(new Callback() {
            @Override
            public void onFailure(@NotNull Call call, @NotNull IOException e) {
                f.completeExceptionally(e);
            }

            @Override
            public void onResponse(@NotNull Call call, @NotNull Response response) throws IOException {
                final ResponseBody body = Objects.requireNonNull(response.body());
                if (response.isSuccessful()) {
                    final IngestResponse resp = GSON.fromJson(body.string(), IngestResponse.class);
                    f.complete(resp);
                } else {
                    final ScopeDBException e = ScopeDBException.fromResponse(response.code(), body.string());
                    f.completeExceptionally(e);
                }
            }
        });

        return f;
    }

    private Request makeStatementRequest(StatementRequest request) {
        final HttpUrl url = HttpUrl.Companion.get(config.getEndpoint())
                .newBuilder()
                .addPathSegments("v1/statements")
                .build();
        final RequestBody body = RequestBody.create(GSON.toJson(request), JSON);
        return new Request.Builder().url(url).post(body).build();
    }

    private Request makeFetchStatementRequest(FetchStatementParams params) {
        final HttpUrl url = HttpUrl.Companion.get(config.getEndpoint())
                .newBuilder()
                .addPathSegments("v1/statements")
                .addPathSegment(params.getStatementId())
                .addQueryParameter("format", params.getFormat().toParam())
                .build();
        return new Request.Builder().url(url).build();
    }

    private Request makeIngestRequest(String rows, String statement) {
        final HttpUrl url = HttpUrl.Companion.get(config.getEndpoint())
                .newBuilder()
                .addPathSegments("v1/ingest")
                .build();
        final IngestRequest request = IngestRequest.builder()
                .statement(statement)
                .data(IngestData.builder().rows(rows).build())
                .build();
        final RequestBody body = RequestBody.create(GSON.toJson(request), JSON);
        return new Request.Builder().url(url).post(body).build();
    }

    private CompletableFuture<StatementResponse> execute(StatementRequest request, boolean forget) {
        final CompletableFuture<StatementResponse> f = new CompletableFuture<>();

        final Request req = makeStatementRequest(request);
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
                    if (resp.getStatus() != StatementStatus.Finished && !forget) {
                        final String statementId = resp.getStatementId();
                        final FetchStatementParams params = FetchStatementParams.builder()
                                .statementId(statementId)
                                .format(request.getFormat())
                                .build();
                        ScopeDBClient.this.fetchWith(f, params, false);
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

    private void fetchWith(CompletableFuture<StatementResponse> f, FetchStatementParams params, boolean forget) {
        final Request req = makeFetchStatementRequest(params);

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
                    if (resp.getStatus() != StatementStatus.Finished && !forget) {
                        ScopeDBClient.this.fetchWith(f, params, false);
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
