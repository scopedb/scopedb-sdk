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

package io.scopedb.sdk.client.request;

import com.google.gson.annotations.SerializedName;

public enum ResultFormat {
    /**
     * Arrow format with variant rendered as JSON (BASE64 encoded).
     */
    @SerializedName("arrow-json")
    ArrowJson;

    public String toParam() {
        switch (this) {
            case ArrowJson:
                return "arrow-json";
            default:
                throw new IllegalArgumentException("Unknown format: " + this);
        }
    }
}
