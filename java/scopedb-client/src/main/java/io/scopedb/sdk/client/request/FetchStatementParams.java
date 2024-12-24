package io.scopedb.sdk.client.request;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Builder
@Data
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class FetchStatementParams {
    private final String statementId;
    private final ResultFormat format;
}
