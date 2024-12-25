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
import com.google.gson.JsonSyntaxException;
import lombok.Data;
import lombok.Getter;

public class ScopeDBException extends Exception {
    private static final Gson GSON = new Gson();

    private ScopeDBException(String message) {
        super(message);
    }

    @Getter
    public static class Server extends ScopeDBException {
        private final int statusCode;
        private final Code errorCode;

        private Server(int statusCode, Code errorCode, String message) {
            super(String.format("%d [%s]: %s", statusCode, errorCode, message));
            this.statusCode = statusCode;
            this.errorCode = errorCode;
        }

        public enum Code {
            Unexpected,
            NotFound,
            AlreadyExists,
        }

        @Data
        private static class Response {
            private final String code;
            private final String message;
        }
    }

    @Getter
    public static class Client extends ScopeDBException {
        private final int statusCode;

        private Client(int statusCode, String message) {
            super(String.format("%d: %s", statusCode, message));
            this.statusCode = statusCode;
        }
    }


    public static ScopeDBException fromResponse(int statusCode, String body) {
        try {
            if (body != null) {
                final Server.Response response = GSON.fromJson(body, Server.Response.class);
                final Server.Code errorCode = Server.Code.valueOf(response.code);
                return new ScopeDBException.Server(statusCode, errorCode, response.message);
            }
        } catch (JsonSyntaxException ignored) {
            // passthrough
        }
        return new ScopeDBException.Client(statusCode, body);
    }
}
