package org.pragmaticminds.crunch.api.trigger.strategy;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;

import java.util.HashSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.09.2018
 */
public class LambdaTriggerStrategyTest {
    
    private LambdaTriggerStrategy strategy;
    private LambdaTriggerStrategy clone;
    
    @Before
    public void setUp() throws Exception {
        strategy = new LambdaTriggerStrategy(record -> true, HashSet::new);
        clone = ClonerUtil.clone(strategy);
    }
    
    @Test
    public void isToBeTriggered() {
        assertTrue(strategy.isToBeTriggered(null));
        assertTrue(clone.isToBeTriggered(null));
    }
    
    @Test
    public void getChannelIdentifiers() {
        assertEquals(new HashSet<>(), strategy.getChannelIdentifiers());
        assertEquals(new HashSet<>(), clone.getChannelIdentifiers());
    }
}