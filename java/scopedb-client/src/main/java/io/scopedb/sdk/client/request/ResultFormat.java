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
