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

package io.scopedb.sdk.client.exception;

import com.google.gson.Gson;
import lombok.Data;
import lombok.Getter;

@Getter
public class ScopeDBException extends Exception {
    private static final Gson GSON = new Gson();

    private final int statusCode;
    private final Code errorCode;

    private ScopeDBException(int statusCode, Code errorCode, String message) {
        super(renderErrorMessage(errorCode, message));
        this.statusCode = statusCode;
        this.errorCode = errorCode;
    }

    private static String renderErrorMessage(Code errorCode, String message) {
        return String.format("%s: %s", errorCode, message);
    }

    public enum Code {
        Unexpected,
        NotFound,
        AlreadyExists,
    }

    public static ScopeDBException fromResponse(int statusCode, String body) {
        final Response response = GSON.fromJson(body, Response.class);
        return new ScopeDBException(statusCode, Code.valueOf(response.code), response.message);
    }

    @Data
    private static class Response {
        private final String code;
        private final String message;
    }
}
