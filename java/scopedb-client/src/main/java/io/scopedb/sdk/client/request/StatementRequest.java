package io.scopedb.sdk.client.request;

import com.google.gson.annotations.SerializedName;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Builder
@Data
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class StatementRequest {
    @SerializedName("statement")
    private final String statement;

    @SerializedName("wait_timeout")
    private final String waitTimeout;

    @SerializedName("format")
    private final ResultFormat format;
}
