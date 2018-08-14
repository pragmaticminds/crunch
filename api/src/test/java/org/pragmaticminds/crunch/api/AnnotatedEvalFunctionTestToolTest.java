package org.pragmaticminds.crunch.api;

import org.junit.Test;
import org.pragmaticminds.crunch.api.evaluations.annotated.RegexFind2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Test for testing class of {@link AnnotatedEvalFunction} classes
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 25.10.2017
 */
public class AnnotatedEvalFunctionTestToolTest {
    private static final Logger logger = LoggerFactory.getLogger(AnnotatedEvalFunctionTestToolTest.class);

    @Test
    public void execute() throws Exception {
        AnnotatedEvalFunctionTestTool testTool = new AnnotatedEvalFunctionTestTool(RegexFind2.class);
        List<Object> parameters = new ArrayList<>();
        parameters.add("test123");

        List<List<Object>> inChannels = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            List<Object> channels = new ArrayList<>();
            channels.add("test1" + i);
            inChannels.add(channels);
        }

        EvalFunctionTestTool.EvaluationTestToolEvents results = testTool.execute(parameters, inChannels);
        results.getOutputs().forEach(output -> logger.debug("output: {}", output));
        results.getEvents().forEach(event -> logger.debug("event: {}", event));

        assertEquals(1, results.getEvents().size());
        assertEquals(100, results.getOutputs().size());
        assertEquals(1, results.getOutputs().stream().filter(Objects::nonNull).collect(Collectors.toList()).size());
    }
}