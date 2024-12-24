package io.scopedb.sdk.client.request;

import com.google.gson.annotations.SerializedName;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Builder
@Data
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ResultSet {
    @SerializedName("metadata")
    private final ResultSetMetadata metadata;

    @SerializedName("rows")
    private final String rows;
}
