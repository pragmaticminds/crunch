package org.pragmaticminds.crunch.runtime.merge;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.junit.Test;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.runtime.sort.ValueEventAssigner;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Tests for ValuesMerge Function.
 *
 * @author julian
 * Created by julian on 03.11.17
 */
public class ValuesMergeFunctionIT {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(ValuesMergeFunctionIT.class);

    /**
     * lets process sleep for a specified duration
     *
     * @param duration duration in microseconds
     */
    public static void sleep(int duration) {
        try {
            TimeUnit.MILLISECONDS.sleep(duration);
        } catch (InterruptedException e) {
            logger.warn("Sleep interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

    @Test
    public void map() {
        ValuesMergeFunction mergeFunction = new ValuesMergeFunction();

        TypedValues values1 = TypedValues.builder()
                .source("source1")
                .timestamp(100)
                .values(Collections.singletonMap("key", Value.of("value1")))
                .build();

        TypedValues values2 = TypedValues.builder()
                .source("source1")
                .timestamp(110)
                .values(Collections.singletonMap("key", Value.of("value2")))
                .build();

        TypedValues mergedValues = mergeFunction.mapWithoutState(values1, values2);

        assertEquals(110, mergedValues.getTimestamp());
        assertEquals("value2", mergedValues.getString("key"));
    }

    @Test
    public void map_aggregateMultipleKeys() {
        ValuesMergeFunction mergeFunction = new ValuesMergeFunction();

        TypedValues values1 = TypedValues.builder()
                .source("source1")
                .timestamp(100)
                .values(Collections.singletonMap("key1", Value.of("value1")))
                .build();

        TypedValues values2 = TypedValues.builder()
                .source("source1")
                .timestamp(110)
                .values(Collections.singletonMap("key2", Value.of("value2")))
                .build();

        TypedValues mergedValues = mergeFunction.mapWithoutState(values1, values2);

        assertEquals(110, mergedValues.getTimestamp());
        assertEquals("value1", mergedValues.getString("key1"));
        assertEquals("value2", mergedValues.getString("key2"));
    }

    /**
     * This is a test for the state ahndling and checkpointing.
     * The State is checkpointed every 10 ms.
     * - A source emits one element each 10 ms
     * - This is merged in the merger
     * - on the 50th element an exception is thrown which leads to a stream restart
     * <p>
     * At the end it is checked that all 100 elements where merged correctly.
     *
     * @throws Exception
     */
    @Test
    public void testMerging() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(1);

        // Event Time Processing
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(10);

        // results are collected in a static variable
        ValuesMergeFunctionIT.CollectSink.values.clear();

        // create a stream of custom elements and apply transformations
        ArrayList<MRecord> input = new ArrayList<>();

        for (long i = 0; i < 10; i++) {
            input.add(TypedValues.builder()
                    .source("source1")
                    .timestamp(i)
                    .values(Collections.singletonMap("key" + i, Value.of(i)))
                    .build());
        }

        SingleOutputStreamOperator<MRecord> stream = env.addSource(new SlowSource(input))
                .assignTimestampsAndWatermarks(new ValueEventAssigner(15))
                .map(new ErrorThrower())
                .keyBy(untypedValues -> 1L)
                .map(new ValuesMergeFunction());

        // Add sinks
        stream.addSink(new ValuesMergeFunctionIT.CollectSink());
        stream.addSink(new SinkFunction<MRecord>() {
            @Override
            public void invoke(MRecord value, Context ctx) {
                System.out.println("Current timestamp: " + value.getTimestamp());
            }
        });

        // execute
        env.execute();

        // verify your results
        MRecord lastEvent = CollectSink.values.get(CollectSink.values.size() - 1);

        // If the last Event contains 10 keys than all state is merged of all 10 events.
        assertEquals(10, lastEvent.getChannels().size());
    }

    /**
     * A map function that does nothing except throwing an exception on the 50th element.
     */
    public static class ErrorThrower implements MapFunction<MRecord, MRecord> {

        private int counter = 0;

        @Override
        public MRecord map(MRecord value) {
            if (counter++ == 50) {
                throw new RuntimeException("Here is an exception!");
            }
            return value;
        }
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<MRecord> {

        // must be static
        public static final List<MRecord> values = new ArrayList<>();

        @Override
        public synchronized void invoke(MRecord value) {
            values.add(value);
        }
    }

    /**
     * A slow source that emits one value of the given input list each 10 ms.
     */
    public static class SlowSource implements SourceFunction<MRecord>, ListCheckpointed<Integer> {

        private List<MRecord> input;
        private int counter;

        public SlowSource(List<MRecord> input) {
            this.input = input;
            this.counter = 0;
        }

        @Override
        public void run(SourceContext<MRecord> sourceContext) {
            while (counter < input.size()) {
                MRecord values = input.get(counter);
                sleep(10);
                sourceContext.collect(values);
                counter++;
            }
            sourceContext.close();
        }

        @Override
        public void cancel() {

        }


        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) {
            return Collections.singletonList(counter);
        }

        @Override
        public void restoreState(List<Integer> state) {
            if (state.size() > 0) {
                this.counter = state.get(0);
            } else {
                this.counter = 0;
            }
        }
    }
}