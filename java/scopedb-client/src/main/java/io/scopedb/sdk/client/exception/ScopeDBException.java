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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.Getter;

/**
 * Exception from a ScopeDB server error response.
 */
@Getter
public class ScopeDBException extends Exception {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final int statusCode;

    private ScopeDBException(int statusCode, String message) {
        super(String.format("%d: %s", statusCode, message));
        this.statusCode = statusCode;
    }

    @Data
    private static class Response {
        private final String message;
    }

    public static ScopeDBException of(int statusCode, String body) {
        if (body == null) {
            return new ScopeDBException(statusCode, null);
        }

        try {
            final Response response = MAPPER.readValue(body, Response.class);
            return new ScopeDBException(statusCode, response.message);
        } catch (JsonProcessingException e) {
            // passthrough
        }
        return new ScopeDBException(statusCode, body);
    }
}
