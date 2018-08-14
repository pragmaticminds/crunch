package org.pragmaticminds.crunch.runtime.eval;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.Ignore;
import org.junit.Test;
import org.pragmaticminds.crunch.api.AnnotatedEvalFunctionWrapper;
import org.pragmaticminds.crunch.api.EvalFunction;
import org.pragmaticminds.crunch.api.EvalFunctionCall;
import org.pragmaticminds.crunch.api.annotations.ChannelValue;
import org.pragmaticminds.crunch.api.evaluations.annotated.RegexFind2;
import org.pragmaticminds.crunch.api.events.EventHandler;
import org.pragmaticminds.crunch.api.function.def.*;
import org.pragmaticminds.crunch.api.holder.Holder;
import org.pragmaticminds.crunch.api.mql.DataType;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.ValueEvent;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.Event;
import org.pragmaticminds.crunch.runtime.merge.ValuesMergeFunctionIT;
import org.pragmaticminds.crunch.runtime.sort.ValueEventAssigner;

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
public class EvalFunctionWrapperIT {

    public static final String SOURCE = "testMachine";

    // Complete Test, in the flink framework
    @Test
    @Ignore
    public void testEvaluation() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        env.setParallelism(1);

        // Event Time Processing
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(10);
        env.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());

        // values are collected in a static variable
        EvalFunctionWrapperIT.CollectSink.values.clear();

        // create a stream of custom elements and apply transformations
        ArrayList<TypedValues> input = new ArrayList<>();

        for (long i = 0; i < 10; i++) {
            input.add(TypedValues.builder()
                    .source("source1")
                    .timestamp(i)
                    .values(Collections.singletonMap("DB_channel", Value.of(i)))
                    .source(SOURCE)
                    .build());
        }

        // Mock the EvalFunction and the EvalFunction call
        EvalFunction evalFunction = new AnnotatedEvalFunctionWrapper<>(RegexFind2.class);
        EvalFunctionCall evalFunctionCall = new EvalFunctionCall(evalFunction, Collections.singletonMap("regex", Value.of(".*")), Collections.singletonMap("value", "DB_channel"));

        EvalFunction evalFunction1 = new COUNT();

        EvalFunctionCall evalFunctionCall1 = new EvalFunctionCall(evalFunction1, Collections.emptyMap(), Collections.singletonMap("channel1", "DB_channel"));

        KeyedStream<TypedValues, Long> stream1 = env.fromCollection(input)
                .map(untypedValues -> (ValueEvent) untypedValues)
                .assignTimestampsAndWatermarks(new ValueEventAssigner(15))
                .map(untypedValues -> (TypedValues) untypedValues)
                .keyBy(untypedValues -> 1L);
        stream1.print();

        // Eval Function 1
        stream1
                .process(new EvalFunctionWrapper(evalFunctionCall))
                .addSink(new EvalFunctionWrapperIT.CollectSink());

        // Eval Function 2
        stream1
                .process(new EvalFunctionWrapper(evalFunctionCall1))
                .addSink(new EvalFunctionWrapperIT.CollectSink());

        // execute
        env.execute();

//        // Assert that the last emitted event contains all 1000 keys send in
        System.out.println(CollectSink.values);
        assertEquals(20, CollectSink.values.size());

        // Assert that they have the right source
        assertEquals(SOURCE, CollectSink.values.get(0).getSource());
    }

    @Test
    @Ignore
    public void testRestartOfEvaluation() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // configure your test environment
        environment.setParallelism(1);

        // Event Time Processing
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        environment.enableCheckpointing(10);
        // env.setRestartStrategy(new RestartStrategies.RestartStrategyConfiguration());

        // results are collected in a static variable
        EvalFunctionWrapperIT.CollectSink.values.clear();

        // create a stream of custom elements and apply transformations
        ArrayList<TypedValues> input = new ArrayList<>();

        for (long i = 0; i < 10; i++) {
            input.add(TypedValues.builder()
                    .source("source1")
                    .timestamp(i)
                    .values(Collections.singletonMap("DB_channel", Value.of(i)))
                    .build());
        }

        // Mock the EvalFunction and the EvalFunction call
        EvalFunction evalFunction1 = new COUNT();
        EvalFunctionCall evalFunctionCall = new EvalFunctionCall(evalFunction1, Collections.emptyMap(), Collections.singletonMap("channel1", "DB_channel"));

        KeyedStream<TypedValues, Long> stream = environment.addSource(new ValuesMergeFunctionIT.SlowSource(input))
                .map(untypedValues -> (ValueEvent) untypedValues)
                .assignTimestampsAndWatermarks(new ValueEventAssigner(15))
                .map(untypedValues -> (TypedValues) untypedValues)
                .map(new ValuesMergeFunctionIT.ErrorThrower())
                .keyBy(untypedValues -> 1L);

        stream.print();

        // Eval Function 1
        SingleOutputStreamOperator<Event> stream1 = stream
                .keyBy(untypedValues -> 1L)
                .process(new EvalFunctionWrapper(evalFunctionCall));

        stream1.print();
        stream1.addSink(new EvalFunctionWrapperIT.CollectSink());

        // execute
        environment.execute();

//        // Assert that the last emitted event contains all 1000 keys send in
        System.out.println(EvalFunctionWrapperIT.CollectSink.values);
        assertEquals(10, EvalFunctionWrapperIT.CollectSink.values.size());

    }

    // create a testing sink
    private static class CollectSink implements SinkFunction<Event> {

        // must be static
        public static final List<Event> values = new ArrayList<>();

        @Override
        public synchronized void invoke(Event value) {
            values.add(value);
        }
    }


    //    @ResultTypes(resultTypes = @ResultType(name = "count", dataType = DataType.LONG))
    static class COUNT extends EvalFunction<Void> {

        @ChannelValue(name = "channel1", dataType = DataType.STRING)
        Holder<String> channel1;

        private int count;

        @Override
        public FunctionDef getFunctionDef() {
            return new FunctionDef(new Signature("FUN1", new FunctionParameter("channel1", FunctionParameterType.CHANNEL, DataType.STRING)), this.getClass(), new FunctionResults());
        }

        @Override
        public void setup(Map literals, EventHandler eventHandler) {
            count = 0;
            setEventHandler(eventHandler);
        }

        @Override
        public Void eval(long time, Map channels) {
            count++;
            EventHandler handler = getEventHandler();
            handler.fire(handler.getBuilder()
                    .withTimestamp(Instant.now().toEpochMilli())
                    .withParameter("count", Value.of(count))
                    .withParameter("time", Value.of(time))
                    .build());
            return null;
        }

        @Override
        public void finish() {
            System.out.println("finished");
        }
    }


}