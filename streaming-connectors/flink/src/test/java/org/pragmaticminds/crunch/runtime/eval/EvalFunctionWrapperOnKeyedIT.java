package org.pragmaticminds.crunch.runtime.eval;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Test;
import org.pragmaticminds.crunch.api.EvalFunction;
import org.pragmaticminds.crunch.api.EvalFunctionCall;
import org.pragmaticminds.crunch.api.EvalFunctionFactory;
import org.pragmaticminds.crunch.api.annotations.ChannelValue;
import org.pragmaticminds.crunch.api.events.GenericEventHandler;
import org.pragmaticminds.crunch.api.function.def.*;
import org.pragmaticminds.crunch.api.holder.Holder;
import org.pragmaticminds.crunch.api.records.DataType;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.GenericEvent;
import org.pragmaticminds.crunch.runtime.sort.ValueEventAssigner;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * {@link EvalFunctionWrapper} Tests...
 *
 * @author julian
 * Created by julian on 05.11.17
 */
public class EvalFunctionWrapperOnKeyedIT {

    // Complete Test, in the flink framework
    @Test
    public void testEvaluationOnKeyedStream() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(1);

        // GenericEvent Time Processing
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(10);
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());

        // values are collected in a static variable
        EvalFunctionWrapperOnKeyedIT.CollectSink.values.clear();

        // create a stream of custom elements and apply transformations
        List<MRecord> input = new ArrayList<>();

        for (long i = 0; i < 100; i++) {
            input.add(TypedValues.builder()
                    .source("source" + (i % 10))
                    .timestamp(i)
                    .values(Collections.singletonMap("DB_channel", Value.of(i)))
                    .build());
        }

        EvalFunctionCall evalFunctionCall = new EvalFunctionCall(new CountWithState.factory(), Collections.emptyMap(), Collections.singletonMap("channel1", "DB_channel"));

        // Foce it to use MRecord (seems to infer it from the first element otherwise).
        KeyedStream<MRecord, String> stream1 = env.fromCollection(input, TypeInformation.of(MRecord.class))
                .assignTimestampsAndWatermarks(new ValueEventAssigner(15))
                .keyBy(MRecord::getSource);

        // Eval Function 1
        stream1
                .process(new EvalFunctionWrapper(evalFunctionCall))
                .addSink(new EvalFunctionWrapperOnKeyedIT.CollectSink());

        // execute
        env.execute();

//        // Assert that the last emitted event contains all 1000 keys send in
        // Assert that the last 10 Entries contain value 10 (right state).
        List<GenericEvent> subList = CollectSink.values.subList(90, CollectSink.values.size());

        // Assert that they all have value 10
        for (GenericEvent event : subList) {
            assertEquals(10L, (long) event.getParameter("count").getAsLong());
        }
    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<GenericEvent> {

        // must be static
        protected static final List<GenericEvent> values = new ArrayList<>();

        @Override
        public synchronized void invoke(GenericEvent value, Context ctx) {
            values.add(value);
        }
    }


    static class CountWithState extends EvalFunction<Void> {

        @ChannelValue(name = "channel1", dataType = DataType.STRING)
        Holder<String> channel1;

        private int count;

        @Override
        public FunctionDef getFunctionDef() {
            return new FunctionDef(new Signature("FUN1", new FunctionParameter("channel1", FunctionParameterType.CHANNEL, DataType.STRING)), this.getClass(), new FunctionResults());
        }

        @Override
        public void setup(Map literals, GenericEventHandler eventHandler) {
            count = 0;
            setEventHandler(eventHandler);
        }

        @Override
        public Void eval(long time, Map channels) {
            count++;
            GenericEventHandler handler = getEventHandler();
            handler.fire(handler.getBuilder()
                    .withTimestamp(Instant.now().toEpochMilli())
                    .withSource("")
                    .withEvent("")
                    .withParameter("count", Value.of(count))
                    .withParameter("time", Value.of(time))
                    .build());
            return null;
        }

        @Override
        public void finish() {
            System.out.println("finished");
        }

        public static class factory implements EvalFunctionFactory, Serializable {

            @Override
            public EvalFunction create() {
                System.out.println("Creating one instance...");
                return new CountWithState();
            }
        }
    }


}