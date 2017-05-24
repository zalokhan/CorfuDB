package org.corfudb.infrastructure;

import com.codahale.metrics.MetricRegistry;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.infrastructure.management.ConservativeFailureHandlerPolicy;
import org.corfudb.infrastructure.management.IFailureDetectorPolicy;
import org.corfudb.infrastructure.management.IFailureHandlerPolicy;
import org.corfudb.infrastructure.management.PeriodicPollPolicy;
import org.corfudb.util.MetricsUtils;

import java.time.Duration;
import java.util.Map;

import static org.corfudb.util.MetricsUtils.addJVMMetrics;
import static org.corfudb.util.MetricsUtils.isMetricsReportingSetUp;

/**
 * Server Context:
 * <ul>
 * <li>Contains the common node level {@link DataStore}</li>
 * <li>Responsible for Server level EPOCH </li>
 * <li>Should contain common services/utilities that the different Servers in a node require.</li>
 * </ul>
 *
 * Note:
 * It is created in {@link CorfuServer} and then
 * passed to all the servers including {@link NettyServerRouter}.
 *
 * Created by mdhawan on 8/5/16.
 */
public class ServerContext {
    private static final String PREFIX_EPOCH = "SERVER_EPOCH";
    private static final String KEY_EPOCH = "CURRENT";
    private static final String PREFIX_TAIL_SEGMENT = "TAIL_SEGMENT";
    private static final String KEY_TAIL_SEGMENT = "CURRENT";

    /**
     * various duration constants
     */
    public static final Duration SMALL_INTERVAL = Duration.ofMillis(60_000);
    public static final Duration SHUTDOWN_TIMER = Duration.ofSeconds(5);


    @Getter
    private final Map<String, Object> serverConfig;

    @Getter
    private final DataStore dataStore;

    @Getter
    private IServerRouter serverRouter;

    @Getter
    @Setter
    private IFailureDetectorPolicy failureDetectorPolicy;

    @Getter
    @Setter
    private IFailureHandlerPolicy failureHandlerPolicy;

    @Getter
    public static final MetricRegistry metrics = new MetricRegistry();

    public ServerContext(Map<String, Object> serverConfig, IServerRouter serverRouter) {
        this.serverConfig = serverConfig;
        this.dataStore = new DataStore(serverConfig);
        this.serverRouter = serverRouter;
        this.failureDetectorPolicy = new PeriodicPollPolicy();
        this.failureHandlerPolicy = new ConservativeFailureHandlerPolicy();

        // Metrics setup & reporting configuration
        String mp = "corfu.server.";
        synchronized (metrics) {
            if (! isMetricsReportingSetUp(metrics)) {
                addJVMMetrics(metrics, mp);
                MetricsUtils.addCacheGauges(metrics, mp + "datastore.cache.", dataStore.getCache());
                MetricsUtils.metricsReportingSetup(metrics);
            }
        }
    }

    /**
     * The epoch of this router. This is managed by the base server implementation.
     */
    public long getServerEpoch() {
        Long epoch = dataStore.get(Long.class, PREFIX_EPOCH, KEY_EPOCH);
        return epoch == null ? 0 : epoch;
    }

    public void setServerEpoch(long serverEpoch) {
        dataStore.put(Long.class, PREFIX_EPOCH, KEY_EPOCH, serverEpoch);
        // Set the epoch in the router as well.
        //TODO need to figure out if we can remove this redundancy
        serverRouter.setServerEpoch(serverEpoch);
    }

    public long getTailSegment() {
        Long tailSegment = dataStore.get(Long.class, PREFIX_TAIL_SEGMENT, KEY_TAIL_SEGMENT);
        return tailSegment == null ? 0 : tailSegment;
    }

    public void setTailSegment(long tailSegment) {
        dataStore.put(Long.class, PREFIX_TAIL_SEGMENT, KEY_TAIL_SEGMENT, tailSegment);
    }
}
