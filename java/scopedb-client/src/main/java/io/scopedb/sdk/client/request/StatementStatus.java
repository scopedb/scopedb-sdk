package io.scopedb.sdk.client.request;

import com.google.gson.annotations.SerializedName;
import io.scopedb.sdk.client.exception.ScopeDBException;

/**
 * Statement execution status.
 *
 * <p>Failures are exported as {@link ScopeDBException}.
 */
public enum StatementStatus {
    @SerializedName("running")
    Running,
    @SerializedName("finished")
    Finished,
}
