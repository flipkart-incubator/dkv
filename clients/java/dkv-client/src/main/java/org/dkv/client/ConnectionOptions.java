package org.dkv.client;

import lombok.Builder;
import lombok.Setter;

@Builder
@Setter
public class ConnectionOptions {

    private static final String DEF_METRIC_PREFIX = "dkv-client-java";
    private static final long DEFAULT_REQUEST_TIMEOUT = 5000;
    private static final long DEFAULT_CONNECT_TIMEOUT = 5000;

    private Long requestTimeout;
    private Long readTimeout;
    private Long writeTimeout;
    private Long connectTimeout;
    private String metricPrefix;

    private int connectionPoolSize;

    private long getRequestTimeout() {
        return requestTimeout != null ? requestTimeout : DEFAULT_REQUEST_TIMEOUT;
    }

    public long getReadTimeout() {
        return readTimeout != null ? readTimeout : getRequestTimeout();
    }

    public long getWriteTimeout() {
        return writeTimeout != null ? writeTimeout : getRequestTimeout();
    }

    public long getConnectTimeout() {
        return connectTimeout != null ? connectTimeout : DEFAULT_CONNECT_TIMEOUT;
    }

    public String getMetricPrefix() {
        return metricPrefix == null ? DEF_METRIC_PREFIX : metricPrefix.trim();
    }
}
