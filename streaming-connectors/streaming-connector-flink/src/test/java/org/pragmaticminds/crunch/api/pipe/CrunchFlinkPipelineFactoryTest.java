package org.pragmaticminds.crunch.api.pipe;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Assert;
import org.junit.Test;
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
     * @throws Exception
     */
    @Test
    public void create() throws Exception {
        // implement this
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<UntypedValues> valuess = new ArrayList<>();

        // create 100 values
        for (int i = 0; i < 100; i++) {
            UntypedValues values = new UntypedValues();
            // set some values
            values.setPrefix("test" + i);
            values.setSource("testSource");
            values.setTimestamp(System.currentTimeMillis() + i);
            Map<String, Object> vals = new HashMap<>();
            vals.put("test", "testString" + i);
            values.setValues(vals);
            valuess.add(values);
        }


        UntypedValues lastValues = new UntypedValues();
        // set some values
        lastValues.setPrefix("test" + 100);
        lastValues.setSource("testSource");
        lastValues.setTimestamp(System.currentTimeMillis() + 2000);
        Map<String, Object> vals = new HashMap<>();
        vals.put("test", "testString" + 100);
        lastValues.setValues(vals);
        valuess.add(lastValues);

        DataStreamSource<UntypedValues> inSource = env.fromCollection(valuess);
        DataStream<UntypedValues> in = inSource.broadcast();

        List<SubStream> subStreams = new ArrayList<>();

        // create sub streams
        List<EvaluationFunction> evaluationFunctions = new ArrayList<>();

        final AtomicInteger i = new AtomicInteger(0);
        // create evaluation functions
        evaluationFunctions.add(ctx ->
                ctx.collect(new Event(System.currentTimeMillis(), "" + i.addAndGet(1), "testSource"))
        );

        subStreams.add(
                SubStream.builder()
                        .withIdentifier("testSubStream1")
                        .withSortWindow(100L)
                        .withPredicate(values -> true)
                        .withEvaluationFunctions(evaluationFunctions)
                        .build()
        );

        subStreams.add(
                SubStream.builder()
                        .withIdentifier("testSubStream2")
                        .withSortWindow(100L)
                        .withPredicate(values -> true)
                        .withEvaluationFunctions(evaluationFunctions)
                        .build()
        );

        EvaluationPipeline pipeline = EvaluationPipeline.builder()
                .withIdentifier("testPipe")
                .withSubStreams(subStreams)
                .build();


        DataStream<Event> outEventDataStream = new CrunchFlinkPipelineFactory().create(in, pipeline);

        CollectSink collectSink = new CollectSink();
        outEventDataStream.addSink(collectSink);

        JobExecutionResult testJob = env.execute("testJob");

        Assert.assertEquals(200, CollectSink.values.size());
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<Event> {
        // must be static
        public static final List<Event> values = new ArrayList<>();

        @Override
        public synchronized void invoke(Event event) throws Exception {
            values.add(event);
        }
    }
}