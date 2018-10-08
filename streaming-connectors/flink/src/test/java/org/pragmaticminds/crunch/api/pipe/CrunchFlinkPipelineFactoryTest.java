package org.pragmaticminds.crunch.api.pipe;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Assert;
import org.junit.Test;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.events.GenericEvent;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 01.08.2018
 */
public class CrunchFlinkPipelineFactoryTest {
    
    /**
     * Tests if two mostly identical SubStreams are processing 100 {@link UntypedValues} correctly and produce
     * 200 {@link GenericEvent}s
     *
     * @throws Exception some exception
     */
    @Test
    @SuppressWarnings("unchecked") // is manually checked
    public void create() throws Exception {
        // implement this
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        List<SubStream<GenericEvent>> subStreams = new ArrayList<>();
        
        // create sub streams
        List<EvaluationFunction<GenericEvent>> evaluationFunctions = new ArrayList<>();
        List<RecordHandler> recordHandlers = new ArrayList<>();
        
        final AtomicInteger i = new AtomicInteger(0);
        
        // create evaluation functions
        evaluationFunctions.add(
            new LambdaEvaluationFunction(
                ctx -> ctx.collect(
                    new GenericEvent(
                        System.currentTimeMillis(),
                        "test" + i.addAndGet(1),
                        "testSource"
                    )
                ),
                () -> new HashSet<>(Collections.singletonList("test"))
            )
        );
        
        // create 3 no op record handlers
        recordHandlers.add(new NoopRecordHandler());
        recordHandlers.add(new NoopRecordHandler());
        recordHandlers.add(new NoopRecordHandler());
        
        subStreams.add(
                SubStream.<GenericEvent>builder()
                .withIdentifier("testSubStream1")
                .withSortWindow(100L)
                .withPredicate(values -> true)
                .withRecordHandlers(recordHandlers)
                .withEvaluationFunctions(evaluationFunctions)
                .build()
        );
        
        subStreams.add(
                SubStream.<GenericEvent>builder()
                .withIdentifier("testSubStream2")
                .withSortWindow(100L)
                .withPredicate(values -> true)
                .withRecordHandlers(recordHandlers)
                .withEvaluationFunctions(evaluationFunctions)
                .build()
        );
        
        // Create a Pipeline
        EvaluationPipeline<GenericEvent> pipeline = EvaluationPipeline.<GenericEvent>builder()
            .withIdentifier("testPipe")
            .withSubStreams(subStreams)
            .build();
        
        // Convert to Flink Pipeline
        DataStream<MRecord> in = createDataStream(env);
        
        in.print();
        
        DataStream<GenericEvent> outEventDataStream = new CrunchFlinkPipelineFactory<GenericEvent>()
            .create(in, pipeline, GenericEvent.class);
        GenericEventCollectSink collectSink = new GenericEventCollectSink();
        outEventDataStream.addSink(collectSink);
        
        // Has a side effect (CollectSink)
        env.execute("testJob");
        
        Assert.assertEquals(200, GenericEventCollectSink.values.size());
    }
    
    /**
     * Tests if two mostly identical SubStreams are processing 100 {@link UntypedValues} correctly and produce
     * 200 {@link GenericEvent}s
     *
     * @throws Exception some exception
     */
    @Test
    @SuppressWarnings("unchecked") // is manually checked
    public void createWithStringResult() throws Exception {
        // implement this
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        List<SubStream<String>> subStreams = new ArrayList<>();
        
        // create sub streams
        List<EvaluationFunction<String>> evaluationFunctions = new ArrayList<>();
        List<RecordHandler> recordHandlers = new ArrayList<>();
        
        final AtomicInteger i = new AtomicInteger(0);
        
        // create evaluation functions
        evaluationFunctions.add(new LambdaEvaluationFunction(
            ctx -> ctx.collect("I am the result"),
            () -> new HashSet<>(Collections.singleton("test"))
        ));
        
        // create 3 no op record handlers
        recordHandlers.add(new NoopRecordHandler());
        recordHandlers.add(new NoopRecordHandler());
        recordHandlers.add(new NoopRecordHandler());
        
        subStreams.add(
                SubStream.<String>builder()
                .withIdentifier("testSubStream1")
                .withSortWindow(100L)
                .withPredicate(values -> true)
                .withRecordHandlers(recordHandlers)
                .withEvaluationFunctions(evaluationFunctions)
                .build()
        );
        
        subStreams.add(
                SubStream.<String>builder()
                .withIdentifier("testSubStream2")
                .withSortWindow(100L)
                .withPredicate(values -> true)
                .withRecordHandlers(recordHandlers)
                .withEvaluationFunctions(evaluationFunctions)
                .build()
        );
        
        // Create a Pipeline
        EvaluationPipeline<String> pipeline = EvaluationPipeline.<String>builder()
            .withIdentifier("testPipe")
            .withSubStreams(subStreams)
            .build();
        
        // Convert to Flink Pipeline
        DataStream<MRecord> in = createDataStream(env);
        
        in.print();
        
        DataStream<String> outEventDataStream = new CrunchFlinkPipelineFactory()
            .create(in, pipeline, String.class);
        StringCollectSink collectSink = new StringCollectSink();
        outEventDataStream.addSink(collectSink);
        
        // Has a side effect (CollectSink)
        env.execute("testJob");
        
        Assert.assertEquals(200, StringCollectSink.values.size());
    }
    
    private DataStream<MRecord> createDataStream(StreamExecutionEnvironment env) {
        List<MRecord> valuesList = new ArrayList<>();
        
        // create 100 values
        for (int i = 0; i < 100; i++) {
            UntypedValues values = getValueAtTimestamp(i, System.currentTimeMillis() + i);
            valuesList.add(values);
        }
        
        // Add one last value to trigger High Watermark
        UntypedValues lastValues = getValueAtTimestamp(100, System.currentTimeMillis() + 2000);
        valuesList.add(lastValues);
        
        DataStreamSource<MRecord> inSource = env.fromCollection(valuesList);
        return inSource.broadcast();
    }
    
    private UntypedValues getValueAtTimestamp(int i2, long l) {
        UntypedValues lastValues = new UntypedValues();
        // set some values
        lastValues.setPrefix("test");
        lastValues.setSource("testSource");
        lastValues.setTimestamp(l);
        Map<String, Object> vals = new HashMap<>();
        vals.put("test", "testString" + i2);
        lastValues.setValues(vals);
        return lastValues;
    }
    
    // create a testing sink
    private static class GenericEventCollectSink implements SinkFunction<GenericEvent> {
        protected static final List<GenericEvent> values = new ArrayList<>();
        
        @Override
        public synchronized void invoke(GenericEvent event, Context ctx) {
            values.add(event);
        }
    }
    
    // create a testing sink
    private static class StringCollectSink implements SinkFunction<String> {
        protected static final List<String> values = new ArrayList<>();
        
        @Override
        public synchronized void invoke(String event, Context ctx) {
            values.add(event);
        }
    }
    
    public static class NoopRecordHandler extends AbstractRecordHandler {
    
        /**
         * evaluates the incoming {@link TypedValues} and stores it in the inner sink.
         *
         * @param record contains incoming data
         */
        @Override
        public void apply(MRecord record) {
            /* do nothing */
        }
    
        /**
         * Collects all channel identifiers that are used in the record handler
         *
         * @return a {@link List} or {@link Collection} of all channel identifiers
         */
        @Override
        public Set<String> getChannelIdentifiers() {
            return Collections.emptySet();
        }
    }
}