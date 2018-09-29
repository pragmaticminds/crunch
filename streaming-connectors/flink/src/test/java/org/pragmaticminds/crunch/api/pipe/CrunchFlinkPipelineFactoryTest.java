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
import org.pragmaticminds.crunch.events.Event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 01.08.2018
 */
public class CrunchFlinkPipelineFactoryTest {
    
    /**
     * Tests if two mostly identical SubStreams are processing 100 {@link UntypedValues} correctly and produce
     * 200 {@link Event}s
     *
     * @throws Exception some exception
     */
    @Test
    public void create() throws Exception {
        // implement this
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        List<SubStream> subStreams = new ArrayList<>();
        
        // create sub streams
        List<EvaluationFunction> evaluationFunctions = new ArrayList<>();
        List<RecordHandler> recordHandlers = new ArrayList<>();
        
        final AtomicInteger i = new AtomicInteger(0);
        
        // create evaluation functions
        evaluationFunctions.add(ctx ->
                                    ctx.collect(new Event(System.currentTimeMillis(), "" + i.addAndGet(1), "testSource"))
        );
        
        // create 3 no op record handlers
        recordHandlers.add(new NoopRecordHandler());
        recordHandlers.add(new NoopRecordHandler());
        recordHandlers.add(new NoopRecordHandler());
        
        subStreams.add(
            SubStream.builder()
                .withIdentifier("testSubStream1")
                .withSortWindow(100L)
                .withPredicate(values -> true)
                .withRecordHandlers(recordHandlers)
                .withEvaluationFunctions(evaluationFunctions)
                .build()
        );
        
        subStreams.add(
            SubStream.builder()
                .withIdentifier("testSubStream2")
                .withSortWindow(100L)
                .withPredicate(values -> true)
                .withRecordHandlers(recordHandlers)
                .withEvaluationFunctions(evaluationFunctions)
                .build()
        );
        
        // Create a Pipeline
        EvaluationPipeline pipeline = EvaluationPipeline.builder()
            .withIdentifier("testPipe")
            .withSubStreams(subStreams)
            .build();
        
        // Convert to Flink Pipeline
        DataStream<MRecord> in = createDataStream(env);
        
        in.print();
        
        DataStream<Event> outEventDataStream = new CrunchFlinkPipelineFactory().create(in, pipeline);
        CollectSink collectSink = new CollectSink();
        outEventDataStream.addSink(collectSink);
        
        // Has a side effect (CollectSink)
        env.execute("testJob");
        
        Assert.assertEquals(200, CollectSink.values.size());
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
        lastValues.setPrefix("test" + i2);
        lastValues.setSource("testSource");
        lastValues.setTimestamp(l);
        Map<String, Object> vals = new HashMap<>();
        vals.put("test", "testString" + i2);
        lastValues.setValues(vals);
        return lastValues;
    }
    
    // create a testing sink
    private static class CollectSink implements SinkFunction<Event> {
        // must be static
        protected static final List<Event> values = new ArrayList<>();
        
        @Override
        public synchronized void invoke(Event event, Context ctx) {
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
    }
}