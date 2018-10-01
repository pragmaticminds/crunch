package org.pragmaticminds.crunch.api.state;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.pipe.LambdaEvaluationFunction;
import org.pragmaticminds.crunch.api.trigger.comparator.SerializableResultFunction;

import java.util.Collection;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.09.2018
 */
public class CloneStateEvaluationFunctionFactoryTest {
    
    private CloneStateEvaluationFunctionFactory factory;
    
    @Before
    @SuppressWarnings("unchecked") // checked manually
    public void setUp() throws Exception {
        EvaluationFunction function = new LambdaEvaluationFunction(
                context -> {
                },
                (SerializableResultFunction<HashSet<String>>) HashSet::new
        );
        factory = CloneStateEvaluationFunctionFactory.builder()
            .withPrototype(function)
            .build();
    }
    
    @Test
    public void create() {
        EvaluationFunction evaluationFunction = factory.create();
        assertEquals(new HashSet<>(), evaluationFunction.getChannelIdentifiers());
    }
    
    @Test
    public void getChannelIdentifiers() {
        Collection<String> channelIdentifiers = factory.getChannelIdentifiers();
        assertEquals(new HashSet<>(), channelIdentifiers);
    }
}