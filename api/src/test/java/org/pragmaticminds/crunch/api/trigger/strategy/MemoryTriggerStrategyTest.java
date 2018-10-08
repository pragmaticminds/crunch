package org.pragmaticminds.crunch.api.trigger.strategy;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.09.2018
 */
public class MemoryTriggerStrategyTest implements Serializable {
    
    private MemoryTriggerStrategy strategy;
    private MemoryTriggerStrategy clone;
    
    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        Supplier<String> supplier = new InnerSupplier<String>();
        strategy = new InnerMemoryTriggerStrategy(supplier, 10, null);
        clone = ClonerUtil.clone(strategy);
    }
    
    @Test
    public void isToBeTriggered() {
        assertFalse(strategy.isToBeTriggered(null));
        assertFalse(strategy.isToBeTriggered(null));
        assertFalse(strategy.isToBeTriggered(null));
        assertFalse(strategy.isToBeTriggered(null));
        assertTrue(strategy.isToBeTriggered(null));
        assertTrue(strategy.isToBeTriggered(null));
    
        assertFalse(clone.isToBeTriggered(null));
    }
    
    @Test
    public void getChannelIdentifiers() {
        assertTrue(strategy.getChannelIdentifiers().contains("test"));
        assertTrue(clone.getChannelIdentifiers().contains("test"));
    }
    
    public static class InnerMemoryTriggerStrategy<T extends Serializable> extends MemoryTriggerStrategy<T> {
        public InnerMemoryTriggerStrategy(Supplier<T> supplier, int bufferSize, T initialValue) {
            super(supplier, bufferSize, initialValue);
        }
    
        @Override
        public boolean isToBeTriggered(T decisionBase) {
            // triggered when called more than 4 times
            return this.lastDecisionBases.size() > 3;
        }
    }
    
    private class InnerSupplier<T> implements Supplier<String> {
        @Override
        public String extract(MRecord values) {
            return "test";
        }
        
        @Override
        public String getIdentifier() {
            return "test";
        }
        
        @Override
        public Set<String> getChannelIdentifiers() {
            return Collections.singleton("test");
        }
    }
}