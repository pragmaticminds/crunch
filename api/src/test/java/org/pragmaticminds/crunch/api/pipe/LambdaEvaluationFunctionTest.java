package org.pragmaticminds.crunch.api.pipe;

import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.09.2018
 */
public class LambdaEvaluationFunctionTest {

    private final HashSet arrayListMock = mock(HashSet.class);
    private LambdaEvaluationFunction function;
    private boolean evalCalled = false;
    
    @Before
    @SuppressWarnings("unchecked") // manually checked
    public void setUp() throws Exception {
        function = new LambdaEvaluationFunction(
          context -> evalCalled = true,
          () -> arrayListMock
        );
    }
    
    @Test
    public void eval() {
        function.eval(mock(EvaluationContext.class));
        assertTrue(evalCalled);
    }
    
    @Test
    public void getChannelIdentifiers() {
        Collection<String> channelIdentifiers = function.getChannelIdentifiers();
        assertEquals(arrayListMock, channelIdentifiers);
    }
}