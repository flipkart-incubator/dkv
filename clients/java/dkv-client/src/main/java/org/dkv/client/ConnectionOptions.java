package org.dkv.client;

import lombok.Builder;
import lombok.Setter;

@Builder
@Setter
public class ConnectionOptions {

    public static long DEFAULT_REQUEST_TIMEOUT = 5000;
    public static long DEFAULT_CONNECT_TIMEOUT = 5000;

    private Long RequestTimeout;
    private Long ReadTimeout;
    private Long WriteTimeout;
    private Long ConnectTimeout;
    private String metricPrefix;

    private long getRequestTimeout() {
        return RequestTimeout != null ? RequestTimeout : DEFAULT_REQUEST_TIMEOUT;
    }

    public long getReadTimeout() {
        return ReadTimeout != null ? ReadTimeout : getRequestTimeout();
    }

    public long getWriteTimeout() {
        return WriteTimeout != null ? WriteTimeout : getRequestTimeout();
    }

    public long getConnectTimeout() {
        return ConnectTimeout != null ? ConnectTimeout : DEFAULT_CONNECT_TIMEOUT;
    }

    public String getMetricPrefix() {
        return metricPrefix;
    }
}
