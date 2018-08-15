package org.pragmaticminds.crunch.api.state;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.pragmaticminds.crunch.api.pipe.EvaluationFunction;
import org.pragmaticminds.crunch.api.pipe.timer.Timer;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.events.Event;

import java.util.Map;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 07.08.2018
 */
public class StateEvaluationContextTest {
    
    private StateEvaluationContext stateEvaluationContext;
    private TypedValues mockValues;
    private Event event = new Event();
    
    @Before
    public void setUp() throws Exception {
        mockValues = Mockito.mock(TypedValues.class);
        stateEvaluationContext = new StateEvaluationContext(mockValues) {
            @Override
            public Timer createNewTimer(EvaluationFunction evaluationFunction) {
                return null; // no timers are tested here
            }
        };
    }
    
    @Test
    public void fromTypedValues() {
        Assert.assertNotNull(stateEvaluationContext);
    }
    
    @Test
    public void getEvents() {
        stateEvaluationContext.collect("test", event);
        Map<String, Event> events = stateEvaluationContext.getEvents();
        Assert.assertNotNull(events);
        Assert.assertNotEquals(0, events.size());
        Assert.assertEquals(event, events.get("test"));
    }
    
    @Test
    public void get() {
        MRecord typedValues = stateEvaluationContext.get();
        Assert.assertEquals(mockValues, typedValues);
    }
    
    @Test
    public void collect() {
        // no exception is expected
        stateEvaluationContext.collect("test", event);
    }
}