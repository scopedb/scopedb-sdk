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
import dev.failsafe.retrofit.FailsafeCall;
import io.scopedb.sdk.client.arrow.ArrowBatchConvertor;
import io.scopedb.sdk.client.helper.EnumConverterFactory;
import io.scopedb.sdk.client.helper.Futures;
import io.scopedb.sdk.client.request.FetchStatementParams;
import io.scopedb.sdk.client.request.IngestData;
import io.scopedb.sdk.client.request.IngestRequest;
import io.scopedb.sdk.client.request.IngestResponse;
import io.scopedb.sdk.client.request.ResultFormat;
import io.scopedb.sdk.client.request.StatementCancelResponse;
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
import retrofit2.converter.jackson.JacksonConverterFactory;
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

        @POST("/v1/statements/{statement_id}/cancel")
        Call<StatementCancelResponse> cancel(@Path("statement_id") String statementId);
    }

    // Configuration
    private static final Duration MAX_RETRY_DURATION = Duration.ofSeconds(60);
    private static final Duration INITIAL_DELAY = Duration.ofSeconds(1);
    private static final double JITTER = 0.15;

    private final ScopeDBService service;

    public ScopeDBClient(ScopeDBConfig config) {
        final Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(config.getEndpoint())
                .addConverterFactory(JacksonConverterFactory.create())
                .addConverterFactory(new EnumConverterFactory())
                .build();
        this.service = retrofit.create(ScopeDBService.class);
    }

    public CompletableFuture<IngestResponse> ingestArrowBatch(List<VectorSchemaRoot> batches, String statement) {
        final String rows = ArrowBatchConvertor.writeArrowBatch(batches);
        final IngestRequest request = IngestRequest.builder()
                .data(IngestData.Arrow.builder().rows(rows).build())
                .statement(statement)
                .build();

        return executeWithRetry(service.ingest(request), createBasicRetryPolicy());
    }

    public CompletableFuture<StatementCancelResponse> cancel(String statementId) {
        return executeWithRetry(service.cancel(statementId), createBasicRetryPolicy());
    }

    public CompletableFuture<StatementResponse> submit(StatementRequest request, boolean waitUntilDone) {
        if (!waitUntilDone) {
            return executeWithRetry(service.submit(request), createBasicRetryPolicy());
        }

        final CompletableFuture<StatementResponse> future = new CompletableFuture<>();

        executeWithRetry(service.submit(request), createBasicRetryPolicy())
            .whenComplete((response, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                    return;
                }

                if (response.getStatus() == StatementStatus.Finished) {
                    future.complete(response);
                    return;
                }

                final FetchStatementParams params = FetchStatementParams.builder()
                        .statementId(response.getStatementId())
                        .format(request.getFormat())
                        .build();

                fetch(params, true).whenComplete(Futures.forward(future));
            });

        return future;
    }

    public CompletableFuture<StatementResponse> fetch(FetchStatementParams params, boolean retryUntilDone) {
        final Call<StatementResponse> call = service.fetch(params.getStatementId(), params.getFormat());

        if (retryUntilDone) {
            return executeWithRetry(call, createStatementCompletionRetryPolicy());
        } else {
            return executeWithRetry(call, createBasicRetryPolicy());
        }
    }

    private <T> CompletableFuture<T> executeWithRetry(Call<T> call, RetryPolicy<Response<T>> retryPolicy) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        FailsafeCall.with(retryPolicy)
          .compose(call)
          .executeAsync()
          .whenComplete(Futures.translateResponse(future));
        return future;
    }

    private static <T> RetryPolicy<Response<T>> createBasicRetryPolicy() {
        return RetryPolicy.<Response<T>>builder()
                 .withJitter(JITTER)
                 .withMaxDuration(MAX_RETRY_DURATION)
                 .withDelay(INITIAL_DELAY)
                 .build();
    }

    private static RetryPolicy<Response<StatementResponse>> createStatementCompletionRetryPolicy() {
        return RetryPolicy.<Response<StatementResponse>>builder()
                 .withJitter(JITTER)
                 .withMaxDuration(MAX_RETRY_DURATION)
                 .withDelay(INITIAL_DELAY)
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
    }
}
