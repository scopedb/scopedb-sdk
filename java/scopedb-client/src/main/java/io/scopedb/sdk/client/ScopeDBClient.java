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
import io.scopedb.sdk.client.exception.ScopeDBException;
import io.scopedb.sdk.client.request.FetchStatementParams;
import io.scopedb.sdk.client.request.StatementRequest;
import io.scopedb.sdk.client.request.StatementResponse;
import io.scopedb.sdk.client.request.StatementStatus;
import java.io.IOException;
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

    public CompletableFuture<StatementResponse> execute(StatementRequest request) {
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
