package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;


/**
 * This class provides a set of tests to:
 *
 * 1. Validate functional behaviour of stream's address discovery mechanisms, under certain scenarios.
 *    These mechanisms are:
 *       (a) follow backpointers &
 *       (b) stream address maps (bitmaps).
 * 2. Compare performance of both, for the same described scenarios (example: reading in presence of holes,
 * reading a large stream from a fresh runtime).
 */
public class StreamAddressDiscoveryIT extends AbstractIT {

    private CorfuRuntime createDefaultRuntimeUsingFollowBackpointers() {
        CorfuRuntime runtime = createRuntime(DEFAULT_ENDPOINT);
        runtime.getParameters().setFollowBackpointersEnabled(true);
        return runtime;
    }

    private CorfuRuntime createDefaultRuntimeUsingAddressMaps() {
        CorfuRuntime runtime = createRuntime(DEFAULT_ENDPOINT);
        runtime.getParameters().setFollowBackpointersEnabled(false);
        runtime.getParameters().setStreamBatchSize(PARAMETERS.NUM_ITERATIONS_LOW);
        return runtime;
    }

    private long readFromNewRuntimeFollowingBackpointers(String streamName, int expectedSize) {
        return readFromNewRuntime(createDefaultRuntimeUsingFollowBackpointers(), streamName, expectedSize);
    }

    private long readFromNewRuntimeUsingAddressMaps(String streamName, int expectedSize) {
        return readFromNewRuntime(createDefaultRuntimeUsingAddressMaps(), streamName, expectedSize);
    }

    private long readFromNewRuntime(CorfuRuntime rt, String streamName, int expectedSize) {
        try {
            CorfuTable<Integer, String> table = rt.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                    })
                    .setStreamName(streamName)
                    .open();

            long startTime = System.currentTimeMillis();
            assertThat(table.size()).isEqualTo(expectedSize);
            return System.currentTimeMillis() - startTime;
        } finally {
            rt.shutdown();
        }
    }

     /**
     * This test aims to validate a stream rebuilt when holes are present. At the same time it is a very
     * small scale test for benchmarking stream rebuilt in the presence of holes when using backpointers
     * vs. using stream maps.
     *
     * Steps to reproduce this test:
     * - Write 10000 entries to S1.
     * - Write 10000 entries to S2.
     * - Insert a hole for S1.
     * - Write 100 entries to S1.
     *
     * - From a new (fresh) runtime access S1:
     *      - First, using followBackpointers as the address discovery mechanism.
     *      - Second, using streamMaps as the address discovery mechanism.
     *
     * Compare times for both mechanisms, ensure stream maps is faster than following backpointers
     * (which will single step through 10.000 entries)
     *
     * @throws Exception
     */
    @Test
    public void benchMarkStreamRebuiltInPresenceOfHoles() throws Exception {
        final String stream1Name = "stream1";
        final String stream2Name = "stream2";

        // Create Server & Runtime
        Process server = runDefaultServer();
        // Runtime writers
        CorfuRuntime rt1 = createDefaultRuntime();

        try {
            // Write 10K entries on S1 & S2
            CorfuTable<Integer, String> table1 = rt1.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                    })
                    .setStreamName(stream1Name)
                    .open();

            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LARGE; i++) {
                table1.put(i, String.valueOf(i));
            }

            // Write 10K entries on S2
            CorfuTable<Integer, String> table2 = rt1.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                    })
                    .setStreamName(stream2Name)
                    .open();

            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LARGE; i++) {
                table2.put(i, String.valueOf(i));
            }

            // Force a hole for S1
            Token token = rt1.getSequencerView().next(CorfuRuntime.getStreamID(stream1Name)).getToken();
            rt1.getLayoutView().getRuntimeLayout()
                    .getLogUnitClient("tcp://localhost:9000")
                    .fillHole(token);


            // Write 100 more entries for S1
            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
                table1.put(i, String.valueOf(i));
            }

            // Read S1 from new runtime (following backpointers)
            long totalTimeFollowBackpointers = readFromNewRuntimeFollowingBackpointers(stream1Name,
                    PARAMETERS.NUM_ITERATIONS_LARGE);
            System.out.println("**** Total time new runtime to sync 'Stream 1' (following backpointers): "
                    + totalTimeFollowBackpointers);

            // Read S1 from new runtime (retrieving address map)
            long totalTimeAddressMaps = readFromNewRuntimeUsingAddressMaps(stream1Name,
                    PARAMETERS.NUM_ITERATIONS_LARGE);
            System.out.println("**** Total time new runtime to sync 'Stream 1' (address maps): "
                    + totalTimeAddressMaps);

            assertThat(totalTimeAddressMaps).isLessThanOrEqualTo(totalTimeFollowBackpointers);
        } finally {
            rt1.shutdown();
            shutdownCorfuServer(server);
        }
    }

    /**
     * Compare behaviour of multi-threaded writes over a shared runtime
     * and stream rebuilt.
     *
     * - Write 10K entries to S1 (runtime follow backpointers) - rt1w
     * - Write 10K entries to S1 (runtime address maps) - rt2w
     * - Compare behaviour (it is expected to be in the same order)
     * - Read 10K entries new runtime (follow backpointers) - rt1r
     * - Read 10K entries new runtime (address maps) - rt2r
     * - Compare behaviour (it is expected to be better for address maps)
     *
     * @throws Exception
     */
    @Test
    public void benchmarkMultiThreadedPutsSharedRuntime() throws Exception {
        // Create Server & Runtime
        Process server = runDefaultServer();

        // Writer runtime (follow backpointers)
        CorfuRuntime rt1w = createDefaultRuntimeUsingFollowBackpointers();

        // Writer runtime (retrieve stream address map)
        CorfuRuntime rt2w = createDefaultRuntimeUsingAddressMaps();

        // Fixed Thread Pool
        final int numThreads = 10;
        final int numKeys = PARAMETERS.NUM_ITERATIONS_LARGE;

        try {
            System.out.println("**** Start multi-threaded benchmark");

            CorfuTable<Integer, String> table = rt1w.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                    })
                    .setStreamName("streamTable")
                    .open();

            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            Long startTime = System.currentTimeMillis();

            for (int i = 0; i < numKeys; i++) {
                final int value = i;
                executor.submit(() -> {
                    rt1w.getObjectsView().TXBuild().type(TransactionType.WRITE_AFTER_WRITE).build().begin();
                    table.put(value, String.valueOf(value));
                    rt1w.getObjectsView().TXEnd();
                });
            }

            executor.shutdown();
            executor.awaitTermination(2, TimeUnit.MINUTES);
            System.out.println(String.format("**** Multi-threaded puts (%s threads, %s keys) completed in:" +
                            " %s ms (for follow backpointers)",
                    numThreads, numKeys, (System.currentTimeMillis() - startTime)));

            CorfuTable<Integer, String> table2 = rt2w.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                    })
                    .setStreamName("streamTable")
                    .open();

            ExecutorService executor2 = Executors.newFixedThreadPool(numThreads);
            startTime = System.currentTimeMillis();

            for (int i = 0; i < numKeys; i++) {
                final int value = i;
                executor2.submit(() -> {
                    rt2w.getObjectsView().TXBuild().type(TransactionType.WRITE_AFTER_WRITE).build().begin();
                    table2.put(value, String.valueOf(value));
                    rt2w.getObjectsView().TXEnd();
                });
            }

            executor2.shutdown();
            executor2.awaitTermination(2, TimeUnit.MINUTES);
            System.out.println(String.format("**** Multi-threaded puts (%s threads, %s keys) completed in:" +
                            " %s ms (for stream address maps)",
                    numThreads, numKeys, (System.currentTimeMillis() - startTime)));

            // Read from fresh runtime (follow backpointers)
            long totalTimeFollowBackpointers = readFromNewRuntimeFollowingBackpointers("streamTable",
                    numKeys);
            System.out.println("**** Total time new runtime to sync 'Stream 1' (following backpointers): "
                    + totalTimeFollowBackpointers);

            // Read from fresh runtime (stream address map)
            long totalTimeAddressMaps = readFromNewRuntimeUsingAddressMaps("streamTable",
                    numKeys);
            System.out.println("**** Total time new runtime to sync 'Stream 1' (address maps): "
                    + totalTimeAddressMaps);

            assertThat(totalTimeAddressMaps).isLessThanOrEqualTo(totalTimeFollowBackpointers);
        } finally {
            rt1w.shutdown();
            rt2w.shutdown();
            shutdownCorfuServer(server);
        }
    }

    /**
     * Compare behaviour of multi-threaded writes over multiple runtime's
     * and stream rebuilt.
     *
     * @throws Exception
     */
    @Test
    public void benchmarkMultiThreadedPutsMultiClients() throws Exception {
        // Create Server & Runtime
        Process server = runDefaultServer();

        Map<CorfuRuntime, CorfuTable<Integer, String>> runtimeToTable = new HashMap<>();
        List<CorfuRuntime> runtimesFollowBackpointers = new ArrayList<>();
        List<CorfuRuntime> runtimesAddressMaps = new ArrayList<>();

        // Fixed Thread Pool
        final int numThreads = 10;
        final int numClients = 10;
        final int numKeys = PARAMETERS.NUM_ITERATIONS_LARGE;

        for (int i = 0; i < numClients; i++) {
            CorfuRuntime rt = createDefaultRuntimeUsingFollowBackpointers().setCacheDisabled(false);
            CorfuTable<Integer, String> table = rt.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                    })
                    .setStreamName("streamTable")
                    .open();
            runtimeToTable.put(rt, table);
            runtimesFollowBackpointers.add(rt);
        }

        for (int i = 0; i < numClients; i++) {
            CorfuRuntime rt = createDefaultRuntimeUsingAddressMaps();
            CorfuTable<Integer, String> table = rt.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                    })
                    .setStreamName("streamTable")
                    .open();
            runtimeToTable.put(rt, table);
            runtimesAddressMaps.add(rt);
        }

        try {
            System.out.println("**** Start multi-threaded benchmark");

            // RUNTIME FOLLOW BACKPOINTERS (WRITE)
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            Long startTime = System.currentTimeMillis();

            int runtimeIndex = 0;
            for (int i = 0; i < numKeys; i++) {
                if (runtimeIndex >= numClients) {
                    runtimeIndex = 0;
                }
                final CorfuRuntime rt = runtimesFollowBackpointers.get(runtimeIndex);
                CorfuTable<Integer, String> table = runtimeToTable.get(rt);
                final int value = i;
                executor.submit(() -> {
                    rt.getObjectsView().TXBuild().type(TransactionType.WRITE_AFTER_WRITE).build().begin();
                    table.put(value, String.valueOf(value));
                    rt.getObjectsView().TXEnd();
                });
                runtimeIndex++;
            }

            executor.shutdown();
            executor.awaitTermination(2, TimeUnit.MINUTES);
            System.out.println(String.format("**** Multi-threaded puts (%s threads, %s keys) completed in:" +
                            " %s ms (for follow backpointers)",
                    numThreads, numKeys, (System.currentTimeMillis() - startTime)));

            // RUNTIME ADDRESS MAPS (WRITE)
            executor = Executors.newFixedThreadPool(numThreads);
            startTime = System.currentTimeMillis();

            runtimeIndex = 0;
            for (int i = 0; i < numKeys; i++) {
                if (runtimeIndex >= numClients) {
                    runtimeIndex = 0;
                }
                final CorfuRuntime rt = runtimesAddressMaps.get(runtimeIndex);
                CorfuTable<Integer, String> table = runtimeToTable.get(rt);
                final int value = i;
                executor.submit(() -> {
                    rt.getObjectsView().TXBuild().type(TransactionType.WRITE_AFTER_WRITE).build().begin();
                    table.put(value, String.valueOf(value));
                    rt.getObjectsView().TXEnd();
                });
                runtimeIndex++;
            }

            executor.shutdown();
            executor.awaitTermination(2, TimeUnit.MINUTES);
            System.out.println(String.format("**** Multi-threaded puts (%s threads, %s keys) completed in:" +
                            " %s ms (for stream address maps)",
                    numThreads, numKeys, (System.currentTimeMillis() - startTime)));

            // Read from fresh runtime (follow backpointers)
            long totalTimeFollowBackpointers = readFromNewRuntimeFollowingBackpointers("streamTable",
                    numKeys);
            System.out.println("**** Total time new runtime to sync stream (following backpointers): "
                    + totalTimeFollowBackpointers);

            // Read from fresh runtime (stream address map)
            long totalTimeAddressMaps = readFromNewRuntimeUsingAddressMaps("streamTable",
                    numKeys);
            System.out.println("**** Total time new runtime to sync stream (address maps): "
                    + totalTimeAddressMaps);

            assertThat(totalTimeAddressMaps).isLessThanOrEqualTo(totalTimeFollowBackpointers);
        } finally {
            for(CorfuRuntime rt : runtimesFollowBackpointers) {
                rt.shutdown();
            }

            for(CorfuRuntime rt : runtimesAddressMaps) {
                rt.shutdown();
            }

            shutdownCorfuServer(server);
        }
    }

    /**
     * Evaluate stream rebuilt for transactional reads.
     *
     * @throws Exception
     */
    @Test
    public void benchmarkMultiThreadedPutsReadsTx() throws Exception {
        // Create Server & Runtime
        Process server = runDefaultServer();

        // Writer Runtime
        CorfuRuntime rt1 = createDefaultRuntime();

        // Reader Runtime (following backpointers)
        CorfuRuntime rt2 = createDefaultRuntimeUsingFollowBackpointers();

        // Reader Runtime (stream address maps)
        CorfuRuntime rt3 = createDefaultRuntimeUsingAddressMaps();

        // Fixed Thread Pool
        final int numThreads = 10;
        final int numKeys = 10000;

        try {
            System.out.println("Start multi-threaded benchmark");

            CorfuTable<Integer, String> table = rt1.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                    })
                    .setStreamName("streamTable")
                    .open();

            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            ExecutorService executor2 = Executors.newFixedThreadPool(numThreads);
            ExecutorService executor3 = Executors.newFixedThreadPool(numThreads);

            Long startTime = System.currentTimeMillis();

            for (int i = 0; i < numKeys; i++) {
                final int value = i;
                executor.submit(() -> {
                    rt1.getObjectsView().TXBuild().type(TransactionType.OPTIMISTIC).build().begin();
                    table.put(value, String.valueOf(value));
                    rt1.getObjectsView().TXEnd();
                });
            }

            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);

            System.out.println(String.format("**** Multi-threaded puts (%s threads, %s keys) completed in: %s ms",
                    numThreads, numKeys, (System.currentTimeMillis() - startTime)));

            // Read from fresh runtime (following backpointers)
            CorfuTable<Integer, String> table2 = rt2.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                    })
                    .setStreamName("streamTable")
                    .open();

            startTime = System.currentTimeMillis();
            for (int i = 0; i < numKeys; i++) {
                final int value = i;
                executor2.submit(() -> {
                    rt2.getObjectsView().TXBuild().type(TransactionType.OPTIMISTIC).build().begin();
                    assertThat(table2.get(value)).isEqualTo(String.valueOf(value));
                    rt2.getObjectsView().TXEnd();
                });
            }

            executor2.shutdown();
            executor2.awaitTermination(1, TimeUnit.MINUTES);

            long followBackpointersTime = System.currentTimeMillis() - startTime;

            System.out.println(String.format("**** New runtime read (following backpointers) completed in: %s ms",
                    followBackpointersTime));

            assertThat(table2.size()).isEqualTo(numKeys);

            // Read from fresh runtime (stream address map)
            CorfuTable<Integer, String> table3 = rt3.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<Integer, String>>() {
                    })
                    .setStreamName("streamTable")
                    .open();

            startTime = System.currentTimeMillis();
            for (int i = 0; i < numKeys; i++) {
                final int value = i;
                executor3.submit(() -> {
                    rt3.getObjectsView().TXBuild().type(TransactionType.OPTIMISTIC).build().begin();
                    assertThat(table3.get(value)).isEqualTo(String.valueOf(value));
                    rt3.getObjectsView().TXEnd();
                });
            }

            executor3.shutdown();
            executor3.awaitTermination(1, TimeUnit.MINUTES);

            long addressMapTime = System.currentTimeMillis() - startTime;

            System.out.println(String.format("**** New runtime read (stream address maps) completed in: %s ms",
                    addressMapTime));

            assertThat(table3.size()).isEqualTo(numKeys);
            assertThat(addressMapTime).isLessThanOrEqualTo(followBackpointersTime);
        } catch(Exception e) {
            System.out.println("**** Exception: " + e);
            // Exception
        } finally {
            rt1.shutdown();
            rt2.shutdown();
            shutdownCorfuServer(server);
        }
    }

    /**
     *  This test checkpoints two streams separately and trims on the lower checkpoint boundary.
     *  The objective is to test that a stream is rebuilt from a checkpoint with updates to the
     *  regular stream still present in the log (addresses 10 and 11)
     *
     *         S1  S2  S1  S2  S2  S1 cp1 cp1 cp1 s1   s2   s2   cp2  cp2  cp2   s2
     *       +---------------------------------------------------------------------+
     *       | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 |
     *       +---------------------------------------------------------------------+
     *
     *  S1: Stream 1
     *  S2: Stream 2
     *  CP1: Checkpoint S1
     *  CP2: Checkpoint S2
     *
     * @throws Exception
     */
    @Test
    public void checkpointAndTrimAtDifferentPoint() throws Exception {

        Process server = runDefaultServer();
        CorfuRuntime defaultRT = createDefaultRuntime();
        CorfuRuntime rt = createDefaultRuntime();

        try {
            final int sizeMap1 = 4;
            final int sizeMap2 = 6;

            Map<String, String> map1 = defaultRT.getObjectsView().build()
                    .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                    })
                    .setStreamName("stream1")
                    .open();

            Map<String, String> map2 = defaultRT.getObjectsView().build()
                    .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                    })
                    .setStreamName("stream2")
                    .open();

            // Writes as described in the comments
            transactionalWrite(defaultRT, map1, "0", "0");
            transactionalWrite(defaultRT, map2, "1", "1");
            transactionalWrite(defaultRT, map1, "2", "2");
            transactionalWrite(defaultRT, map2, "3", "3");
            transactionalWrite(defaultRT, map2, "4", "4");
            transactionalWrite(defaultRT, map1, "5", "5");

            // Checkpoint S1
            MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
            mcw1.addMap(map1);
            Token minCheckpointAddress = mcw1.appendCheckpoints(defaultRT, "author");

            transactionalWrite(defaultRT, map1, "9", "9");
            transactionalWrite(defaultRT, map2, "10", "10");
            transactionalWrite(defaultRT, map2, "11", "11");

            // Checkpoint S2
            MultiCheckpointWriter mcw2 = new MultiCheckpointWriter();
            mcw2.addMap(map2);
            Token maxCheckpointAddress = mcw2.appendCheckpoints(defaultRT, "author");

            transactionalWrite(defaultRT, map2, "15", "15");

            assertThat(map1).hasSize(sizeMap1);
            assertThat(map2).hasSize(sizeMap2);

            // Trim on the lower address (@5)
            defaultRT.getAddressSpaceView().prefixTrim(minCheckpointAddress);

            // New runtime read s1, read s2 (from checkpoint)
            Map<String, String> map1rt = rt.getObjectsView().build()
                    .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                    })
                    .setStreamName("stream1")
                    .open();

            Map<String, String> map2rt = rt.getObjectsView().build()
                    .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                    })
                    .setStreamName("stream2")
                    .open();

            assertThat(map1rt.size()).isEqualTo(sizeMap1);
            assertThat(map2rt.size()).isEqualTo(sizeMap2);
        } catch (Exception e) {
            fail("Exception thrown", e);
        } finally {
            defaultRT.shutdown();
            rt.shutdown();
            shutdownCorfuServer(server);
        }
    }

    private void transactionalWrite(CorfuRuntime rt, Map<String, String> map, String key, String value) {
        rt.getObjectsView().TXBuild().type(TransactionType.OPTIMISTIC).build().begin();
        map.put(key, value);
        rt.getObjectsView().TXEnd();
    }

    /**
     *
     *    This test validates that a snapshot transaction can be completed
     *    between two checkpoints, whenever part of the address space below the
     *    first checkpoint has been trimmed.
     *
     *
     *         S1  S1  S1  S1  S1  [   CP1   ] S1  S1  S1   S1   S1  [    CP2      ]
     *       +---------------------------------------------------------------------+
     *       | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 14 | 15 |
     *       +---------------------------------------------------------------------+
     *                     ^                       ^
     *                    TRIM                  SNAPSHOT
     *
     *  S1: Stream 1
     *  CP1: Checkpoint 1 to S1
     *  CP2: Checkpoint 2 to S1
     *
     * @throws Exception
     */
    @Test
    public void checkpointAndTrimAtDifferentPointSnapshot() throws Exception {

        Process server = runDefaultServer();
        CorfuRuntime writeRuntime = createDefaultRuntime();
        CorfuRuntime readRuntime = createDefaultRuntime();

        try {
            final int batchWrite = 5;
            final long trimAddress = 3L;
            final long snapshotAddress = 9L;
            final int sizeAtSnapshot = 7;

            Map<String, String> map1 = writeRuntime.getObjectsView().build()
                    .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                    })
                    .setStreamName("stream1")
                    .open();

            for(int i=0; i<batchWrite; i++) {
                transactionalWrite(writeRuntime, map1, String.valueOf(i), String.valueOf(i));
            }

            // Checkpoint 1
            MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
            mcw1.addMap(map1);
            Token minCheckpointAddress = mcw1.appendCheckpoints(writeRuntime, "author");

            for(int i=batchWrite; i<batchWrite + batchWrite; i++) {
                transactionalWrite(writeRuntime, map1, String.valueOf(i), String.valueOf(i));
            }

            // Checkpoint 2
            MultiCheckpointWriter mcw2 = new MultiCheckpointWriter();
            mcw2.addMap(map1);
            Token maxCheckpointAddress = mcw2.appendCheckpoints(writeRuntime, "author");

            assertThat(map1).hasSize(batchWrite+batchWrite);

            // Trim below lower checkpoint
            writeRuntime.getAddressSpaceView().prefixTrim(new Token(minCheckpointAddress.getEpoch(), trimAddress));

            // New runtime
            Map<String, String> map1rt = readRuntime.getObjectsView().build()
                    .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                    })
                    .setStreamName("stream1")
                    .open();

            // Start snapshot transaction between both snapshots
            readRuntime.getObjectsView().TXBuild().type(TransactionType.SNAPSHOT)
                    .snapshot(new Token(maxCheckpointAddress.getEpoch(), snapshotAddress))
                    .build()
                    .begin();
            assertThat(map1rt).hasSize(sizeAtSnapshot);
            readRuntime.getObjectsView()
                    .TXEnd();
        } catch (Exception e) {
            fail("Exception thrown", e);
        } finally {
            writeRuntime.shutdown();
            readRuntime.shutdown();
            shutdownCorfuServer(server);
        }
    }
}
