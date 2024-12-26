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

import dev.failsafe.RetryPolicy;
import dev.failsafe.RetryPolicyBuilder;
import dev.failsafe.retrofit.FailsafeCall;
import io.scopedb.sdk.client.arrow.ArrowBatchConvertor;
import io.scopedb.sdk.client.helper.EnumConverterFactory;
import io.scopedb.sdk.client.helper.FutureUtils;
import io.scopedb.sdk.client.request.FetchStatementParams;
import io.scopedb.sdk.client.request.IngestData;
import io.scopedb.sdk.client.request.IngestRequest;
import io.scopedb.sdk.client.request.IngestResponse;
import io.scopedb.sdk.client.request.ResultFormat;
import io.scopedb.sdk.client.request.StatementRequest;
import io.scopedb.sdk.client.request.StatementResponse;
import io.scopedb.sdk.client.request.StatementStatus;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.arrow.vector.VectorSchemaRoot;
import retrofit2.Call;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.gson.GsonConverterFactory;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Path;
import retrofit2.http.Query;

public class ScopeDBClient {
    private interface ScopeDBService {
        @POST("/v1/ingest")
        Call<IngestResponse> ingest(@Body IngestRequest request);

        @POST("/v1/statements")
        Call<StatementResponse> submit(@Body StatementRequest request);

        @GET("/v1/statements/{statement_id}")
        Call<StatementResponse> fetch(@Path("statement_id") String statementId, @Query("format") ResultFormat format);
    }

    private final ScopeDBService service;

    public ScopeDBClient(ScopeDBConfig config) {
        final Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(config.getEndpoint())
                .addConverterFactory(GsonConverterFactory.create())
                .addConverterFactory(new EnumConverterFactory())
                .build();
        this.service = retrofit.create(ScopeDBService.class);
    }

    public CompletableFuture<IngestResponse> ingestArrowBatch(List<VectorSchemaRoot> batches, String statement) {
        final String rows = ArrowBatchConvertor.writeArrowBatch(batches);
        final IngestRequest request = IngestRequest.builder()
                .data(IngestData.builder().rows(rows).build())
                .statement(statement)
                .build();

        final RetryPolicy<Response<IngestResponse>> retryPolicy = createBasicRetryPolicy();
        final Call<IngestResponse> call = service.ingest(request);

        final CompletableFuture<IngestResponse> f = new CompletableFuture<>();
        FailsafeCall.with(retryPolicy).compose(call).executeAsync().whenComplete(FutureUtils.translateResponse(f));
        return f;
    }

    public CompletableFuture<StatementResponse> submit(StatementRequest request, boolean waitUntilDone) {
        final RetryPolicy<Response<StatementResponse>> retryPolicy = createBasicRetryPolicy();
        final Call<StatementResponse> call = service.submit(request);

        final CompletableFuture<StatementResponse> f = new CompletableFuture<>();
        FailsafeCall.with(retryPolicy).compose(call).executeAsync().whenComplete((r, t) -> {
            if (t != null) {
                f.completeExceptionally(t);
                return;
            }
            if (!waitUntilDone) {
                // return immediately
                FutureUtils.translateResponse(f).accept(r, null);
                return;
            }
            if (!r.isSuccessful()) {
                // all non-200 responses are considered as permanently failed
                FutureUtils.translateResponse(f).accept(r, null);
                return;
            }

            final StatementResponse resp = r.body();
            if (resp == null) {
                f.completeExceptionally(new NullPointerException("empty response body"));
                return;
            }
            if (resp.getStatus() == StatementStatus.Finished) {
                f.complete(resp);
                return;
            }

            final FetchStatementParams params = FetchStatementParams.builder()
                    .statementId(resp.getStatementId())
                    .format(request.getFormat())
                    .build();
            fetch(params, true).whenComplete(FutureUtils.forward(f));
        });
        return f;
    }

    public CompletableFuture<StatementResponse> fetch(FetchStatementParams params, boolean retryUntilDone) {
        final RetryPolicy<Response<StatementResponse>> retryPolicy = createFetchRetryPolicy(retryUntilDone);
        final Call<StatementResponse> call = service.fetch(params.getStatementId(), params.getFormat());

        final CompletableFuture<StatementResponse> f = new CompletableFuture<>();
        FailsafeCall.with(retryPolicy).compose(call).executeAsync().whenComplete(FutureUtils.translateResponse(f));
        return f;
    }

    private static <T> RetryPolicyBuilder<Response<T>> createSharedRetryPolicyBuilder() {
        return RetryPolicy.<Response<T>>builder()
                .withJitter(0.15)
                .withMaxDuration(Duration.ofSeconds(60))
                .withDelay(Duration.ofSeconds(1));
    }

    private static <T> RetryPolicy<Response<T>> createBasicRetryPolicy() {
        final RetryPolicyBuilder<Response<T>> retryPolicyBuilder = createSharedRetryPolicyBuilder();
        return retryPolicyBuilder.build();
    }

    private static RetryPolicy<Response<StatementResponse>> createFetchRetryPolicy(boolean retryUntilDone) {
        final RetryPolicyBuilder<Response<StatementResponse>> retryPolicyBuilder = createSharedRetryPolicyBuilder();
        if (retryUntilDone) {
            return retryPolicyBuilder
                    .handleResultIf(response -> {
                        // statement is not done; retry
                        if (response.isSuccessful()) {
                            final StatementResponse statementResponse = response.body();
                            return statementResponse != null
                                    && statementResponse.getStatus() != StatementStatus.Finished;
                        }

                        // all non-200 responses are considered as permanently failed
                        return false;
                    })
                    .build();
        } else {
            return retryPolicyBuilder.build();
        }
    }
}
