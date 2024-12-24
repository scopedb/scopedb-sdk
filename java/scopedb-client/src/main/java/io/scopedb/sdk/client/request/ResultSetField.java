package io.scopedb.sdk.client.request;

import com.google.gson.annotations.SerializedName;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Builder
@Data
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ResultSetField {
    @SerializedName("name")
    private final String name;

    @SerializedName("data_type")
    private final String dataType;
}
