package io.scopedb.sdk.client.request;

import com.google.gson.annotations.SerializedName;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Builder
@Data
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class StatementResponse {
    @SerializedName("statement_id")
    private final String statementId;

    @SerializedName("status")
    private final StatementStatus status;

    @SerializedName("result_set")
    private final ResultSet resultSet;
}
