package org.pragmaticminds.crunch.api.trigger.strategy;

import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.09.2018
 */
public class LambdaTriggerStrategyTest {
    
    private LambdaTriggerStrategy strategy;
    
    @Before
    public void setUp() throws Exception {
        strategy = new LambdaTriggerStrategy(record -> true, HashSet::new);
    }
    
    @Test
    public void isToBeTriggered() {
        assertTrue(strategy.isToBeTriggered(null));
    }
    
    @Test
    public void getChannelIdentifiers() {
        assertEquals(new HashSet<>(), strategy.getChannelIdentifiers());
    }
}