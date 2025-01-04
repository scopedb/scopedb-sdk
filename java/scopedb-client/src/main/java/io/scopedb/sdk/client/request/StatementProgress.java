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

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.jackson.Jacksonized;

@Builder
@Data
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Jacksonized
public class StatementProgress {
    /**
     * Total progress in percentage: [0.0, 100.0].
     */
    @JsonProperty("total_percentage")
    private final double totalPercentage;

    /**
     * Duration in nanoseconds since the statement is submitted.
     */
    @JsonProperty("nanos_from_submitted")
    private final long nanosFromSubmitted;

    /**
     * Duration in nanoseconds since the statement is started.
     */
    @JsonProperty("nanos_from_started")
    private final long nanosFromStarted;

    /**
     * Duration in nanoseconds for estimated to finish the statement.
     */
    @JsonProperty("nanos_to_finish")
    private final long nanosToFinish;
}
