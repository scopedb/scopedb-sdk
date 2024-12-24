package io.scopedb.sdk.client;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * Configuration for the ScopeDB client.
 */
@Builder
@Data
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ScopeDBConfig {
    /**
     * The URL of the ScopeDB server.
     */
    private final String endpoint;
}
