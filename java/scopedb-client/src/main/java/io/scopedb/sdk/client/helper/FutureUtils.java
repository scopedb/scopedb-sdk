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

package io.scopedb.sdk.client.helper;

import io.scopedb.sdk.client.exception.ScopeDBException;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import lombok.experimental.UtilityClass;
import okhttp3.ResponseBody;
import retrofit2.Response;

@UtilityClass
public class FutureUtils {
    public static <T> BiConsumer<? super T, ? super Throwable> forward(CompletableFuture<T> f) {
        return (r, t) -> {
            if (t != null) {
                f.completeExceptionally(t);
            } else {
                f.complete(r);
            }
        };
    }

    public static <T> BiConsumer<Response<T>, ? super Throwable> translateResponse(CompletableFuture<T> f) {
        return (r, t) -> {
            if (t != null) {
                f.completeExceptionally(t);
                return;
            }

            if (r.isSuccessful()) {
                f.complete(r.body());
                return;
            }

            try (final ResponseBody error = r.errorBody()) {
                f.completeExceptionally(ScopeDBException.of(r.code(), error != null ? error.string() : null));
            } catch (Exception e) {
                f.completeExceptionally(e);
            }
        };
    }
}
