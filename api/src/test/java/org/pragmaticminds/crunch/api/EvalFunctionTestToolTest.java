package org.pragmaticminds.crunch.api;

import org.junit.Test;
import org.pragmaticminds.crunch.api.annotations.*;
import org.pragmaticminds.crunch.api.events.EventHandler;
import org.pragmaticminds.crunch.api.holder.Holder;
import org.pragmaticminds.crunch.api.mql.DataType;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.events.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests the functionality of the {@link EvalFunctionTestTool}
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 23.11.2017
 */
public class EvalFunctionTestToolTest {

    @Test
    public void executeMultipleLiteralsAndChannels() throws Exception {
        EvalFunctionTestTool tester = new EvalFunctionTestTool(new MultipleLiteralsAndChannels().asEvalFunction());

        Map<String, Value> literals = new HashMap<>();
        List<Map<String, Value>> channels = new ArrayList<>();

        // set literals
        literals.put("parameterInstant", Value.of(Date.from(Instant.ofEpochMilli(12345L))));
        literals.put("parameterDouble", Value.of(0.1D));
        literals.put("parameterLong", Value.of(123L));
        literals.put("parameterBoolean", Value.of(true));
        literals.put("parameterString", Value.of("testString"));

        // set channels
        List<Long> dates = new ArrayList<>();
        Long dateCnt = 0L;
        for (int cnt = 0; cnt < 2; cnt++) {
            for (int i = 0; i < 2; i++) {
                HashMap<String, Value> channelValues = new HashMap<>();
                channelValues.put("channelInstant", Value.of(Date.from(Instant.ofEpochMilli(12345L))));
                channelValues.put("channelDouble", Value.of(0.2D));
                channelValues.put("channelLong", Value.of(1234L));
                channelValues.put("channelBoolean", Value.of(false));
                channelValues.put("channelString", Value.of("testString2"));
                channels.add(channelValues);
                dates.add(dateCnt += 10_000);
            }
        }

        EvalFunctionTestTool.EvaluationTestToolEvents events = tester.execute(literals, channels, dates);

        assertEquals(1, events.getEvents().size());

        //check the machine cycles
        List<Event> eventList = events.getEvents();
        for (Event event : eventList) {
            // assertTrue((event.getParameter(Parameters.MACHINE_CYCLE_START).getAsLong() < event.getParameter(Parameters.MACHINE_CYCLE_END).getAsLong()));
            // assertTrue(event.getParameter(Parameters.MACHINE_CYCLE_TIME_ACTUAL).getAsLong() > 0);
            // assertEquals(event.getTimestamp(), event.getParameter(Parameters.MACHINE_CYCLE_START).getAsLong());
        }
    }

    /**
     * This is a test purpose {@link EvalFunction} implementation, with all possible parameter and channels set
     */
    @EvaluationFunction(evaluationName = "MULTIPLE_LITERALS_AND_CHANNELS", dataType = DataType.STRING, description = "only for test purpose.")
    public static class MultipleLiteralsAndChannels implements AnnotatedEvalFunction<String>, Serializable {

        private static final Logger logger = LoggerFactory.getLogger(MultipleLiteralsAndChannels.class);

        @ResultTypes(resultTypes = @ResultType(name = "test", dataType = DataType.STRING))
        private transient EventHandler eventHandler;

        @TimeValue
        private Holder<Long> time;

        @ChannelValue(name = "channelString", dataType = DataType.STRING)
        private Holder<String> channelString;

        @ChannelValue(name = "channelLong", dataType = DataType.LONG)
        private Holder<Long> channelLong;

        @ChannelValue(name = "channelDouble", dataType = DataType.DOUBLE)
        private Holder<Double> channelDouble;

        @ChannelValue(name = "channelInstant", dataType = DataType.TIMESTAMP)
        private Holder<Instant> channelInstant;

        @ChannelValue(name = "channelBoolean", dataType = DataType.BOOL)
        private Holder<Boolean> channelBoolean;


        @ParameterValue(name = "parameterString", dataType = DataType.STRING)
        private Holder<String> parameterString;

        @ParameterValue(name = "parameterLong", dataType = DataType.LONG)
        private Holder<Long> parameterLong;

        @ParameterValue(name = "parameterDouble", dataType = DataType.DOUBLE)
        private Holder<Double> parameterDouble;

        @ParameterValue(name = "parameterInstant", dataType = DataType.TIMESTAMP)
        private Holder<Instant> parameterInstant;

        @ParameterValue(name = "parameterBoolean", dataType = DataType.BOOL)
        private Holder<Boolean> parameterBoolean;

        /**
         * checks all parameter annotated values
         */
        @Override
        public void setup() {
            assertNotNull(parameterString.get());
            assertEquals("testString", parameterString.get());
            assertNotNull(parameterBoolean.get());
            assertEquals(true, parameterBoolean.get());
            assertNotNull(parameterDouble.get());
            assertEquals(0.1D, parameterDouble.get(), 0.0D);
            assertNotNull(parameterInstant.get());
            assertEquals(Instant.ofEpochMilli(12345L), parameterInstant.get());
            assertNotNull(parameterLong.get());
            assertEquals(123L, (long) parameterLong.get());
        }

        /**
         * checks all channel annotated values and the time value
         *
         * @return
         */
        @Override
        public String eval() {
            assertNotNull(channelString.get());
            assertEquals("testString2", channelString.get());
            assertNotNull(channelBoolean.get());
            assertEquals(false, channelBoolean.get());
            assertNotNull(channelDouble.get());
            assertEquals(0.2D, channelDouble.get(), 0.0D);
            assertNotNull(channelInstant.get());
            assertEquals(Instant.ofEpochMilli(12345L), channelInstant.get());
            assertNotNull(channelLong.get());
            assertEquals(1234L, (long) channelLong.get());

            assertNotNull(time.get());
            return "all parameter and channels are set";
        }

        /**
         * creates an event with all possible value types
         */
        @Override
        public void finish() {
            eventHandler.fire(eventHandler.getBuilder()
                    .withTimestamp(123L)
                    .withEvent("0815")
                    .withSource("none")
                    .withParameter("long", 1L)
                    .withParameter("string", "test")
                    .withParameter("double", 0.3D)
                    .withParameter("date", Date.from(Instant.ofEpochMilli(12345L)))
                    .withParameter("boolean", false)
                    .build());
        }
    }
}