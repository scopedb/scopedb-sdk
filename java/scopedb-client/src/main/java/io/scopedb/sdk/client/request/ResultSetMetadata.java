package io.scopedb.sdk.client.request;

import com.google.gson.annotations.SerializedName;
import java.util.List;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Builder
@Data
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class ResultSetMetadata {
    @SerializedName("fields")
    private final List<ResultSetField> fields;
}
