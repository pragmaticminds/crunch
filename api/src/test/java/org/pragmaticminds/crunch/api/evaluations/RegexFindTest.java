package org.pragmaticminds.crunch.api.evaluations;

import org.junit.Test;
import org.pragmaticminds.crunch.api.EvalFunctionTestTool;
import org.pragmaticminds.crunch.api.values.dates.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Searches for the appearance of given regex in the given channel
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 19.10.2017
 */
public class RegexFindTest {
    private static final Logger logger = LoggerFactory.getLogger(RegexFindTest.class);

    @Test
    public void evaluate() throws Exception {
        EvalFunctionTestTool regexFindTester = new EvalFunctionTestTool(RegexFind.class);

        Map<String, Value> literals = new HashMap<>();
        literals.put("regex", Value.of("test123"));
        List<Map<String, Value>> channels = new ArrayList<>();
        List<Long> dates = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            HashMap<String, Value> channelValues = new HashMap<>();
            channelValues.put("value", Value.of("test1" + i));
            channels.add(channelValues);
            dates.add(Instant.now().toEpochMilli());
        }
        EvalFunctionTestTool.EvaluationTestToolEvents events = regexFindTester.execute(literals, channels, dates);

        if (logger.isDebugEnabled()) {
            events.getOutputs().forEach(output -> {
                logger.debug("output: {}", output);
            });
            events.getEvents().forEach(event -> {
                logger.debug("event: {}", event);
            });
        }

        assertEquals(100, events.getOutputs().size());
        assertEquals(1, events.getEvents().size());
    }
}