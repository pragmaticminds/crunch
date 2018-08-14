package org.pragmaticminds.crunch.api.state;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 07.08.2018
 */
public class CloneEvaluationFunctionStateFactoryTest {
    
    private CloneStateEvaluationFunctionFactory evaluationFunctionStateFactory;
    
    @Before
    public void setUp() throws Exception {
        CloneStateEvaluationFunctionFactory.Builder builder = CloneStateEvaluationFunctionFactory.builder();
        Assert.assertNotNull(builder);
        builder.withPrototype(Mockito.mock(EvaluationFunction.class));
        evaluationFunctionStateFactory = builder.build();
    }
    
    @Test
    public void builder() {
        Assert.assertNotNull(evaluationFunctionStateFactory);
    }
    
    @Test
    public void create() {
        EvaluationFunction evaluationFunction = evaluationFunctionStateFactory.create();
        Assert.assertNotNull(evaluationFunction);
    }
}