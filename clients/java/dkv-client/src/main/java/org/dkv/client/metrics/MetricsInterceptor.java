package org.dkv.client.metrics;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.grpc.*;

public class MetricsInterceptor implements ClientInterceptor {
    private final MetricRegistry metrics;

    public MetricsInterceptor(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> methodDescriptor,
                                                               CallOptions callOptions, Channel channel) {
        String grpcName = methodDescriptor.getFullMethodName().replace('/', '.');
        Meter requests = metrics.meter(grpcName + ".requests");
        Meter errors = metrics.meter(grpcName + ".errors");
        Timer timer = metrics.timer(grpcName + ".latencies");
        return new MonitoringClientCall<>(
                channel.newCall(methodDescriptor, callOptions), requests, errors, timer);
    }

    private static class MonitoringClientCall<ReqT, RespT> extends ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT> {
        private final Meter requests;
        private final Meter errors;
        private final Timer timer;

        public MonitoringClientCall(ClientCall<ReqT, RespT> clientCall, Meter requests, Meter errors, Timer timer) {
            super(clientCall);
            this.requests = requests;
            this.errors = errors;
            this.timer = timer;
        }

        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
            requests.mark();
            super.start(new MonitoringClientCallListener<>(responseListener, errors, timer.time()), headers);
        }
    }

    private static class MonitoringClientCallListener<RespT> extends ForwardingClientCallListener<RespT> {
        private final ClientCall.Listener<RespT> delegate;
        private final Meter errors;
        private final Timer.Context responseTimer;

        public MonitoringClientCallListener(ClientCall.Listener<RespT> delegate, Meter errors, Timer.Context responseTimer) {
            this.delegate = delegate;
            this.errors = errors;
            this.responseTimer = responseTimer;
        }

        @Override
        protected ClientCall.Listener<RespT> delegate() {
            return delegate;
        }

        @Override
        public void onClose(Status status, Metadata trailers) {
            responseTimer.stop();
            if (!status.isOk()) errors.mark();
            super.onClose(status, trailers);
        }
    }
}
