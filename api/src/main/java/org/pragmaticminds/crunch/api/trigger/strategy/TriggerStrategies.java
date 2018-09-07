package org.pragmaticminds.crunch.api.trigger.strategy;

import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;
import org.pragmaticminds.crunch.api.values.TypedValues;


/**
 * A collection of {@link TriggerStrategy}s for boolean decision making.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 13.08.2018
 */
public class TriggerStrategies {
    private TriggerStrategies() { /* does nothing */}
    
    /**
     * This method triggers always on supplied value = true
     * @param supplier extracts the relevant values from a {@link TypedValues}
     * @return true if triggered
     */
    public static TriggerStrategy onTrue(Supplier<Boolean> supplier){
        return values -> supplier.extract(values) != null && supplier.extract(values);
    }
    
    /**
     * This method triggers always on supplied value = false
     * @param supplier extracts the relevant values from a {@link TypedValues}
     * @return true if triggered, false if not or supplied value is null
     */
    public static TriggerStrategy onFalse(Supplier<Boolean> supplier){
        return values ->
            // null check
            supplier.extract(values) != null
            // on false check
            && !supplier.extract(values);
    }
    
    /**
     * This method triggers on supplied value = true and the last supplied value = false
     * @param supplier extracts the relevant values from a {@link TypedValues}
     * @return true if triggered
     */
    public static TriggerStrategy onBecomeTrue(Supplier<Boolean> supplier){
        return new MemoryTriggerStrategy<Boolean>(supplier, 1) {
            @Override
            public boolean isToBeTriggered(Boolean decisionBase) {
                return
                    // null check
                    decisionBase != null
                    // condition check
                    && !lastDecisionBases.isEmpty() && !lastDecisionBases.get(0) && decisionBase;
            }
        };
    }
    
    /**
     * This method triggers on supplied value = false and the last supplied value = true
     * @param supplier extracts the relevant values from a {@link TypedValues}
     * @return true if triggered
     */
    public static TriggerStrategy onBecomeFalse(Supplier<Boolean> supplier){
        return new MemoryTriggerStrategy<Boolean>(supplier, 1) {
            @Override
            public boolean isToBeTriggered(Boolean decisionBase) {
                return
                    // null check
                    decisionBase != null
                    // condition check
                    && !lastDecisionBases.isEmpty() && lastDecisionBases.get(0) && !decisionBase;
            }
        };
    }
    
    /**
     * This method triggers on supplied value != last supplied value
     * @param supplier extracts the relevant values from a {@link TypedValues}
     * @return true if triggered
     */
    public static TriggerStrategy onChange(Supplier<Boolean> supplier){
        return new MemoryTriggerStrategy<Boolean>(supplier, 1) {
            @Override
            public boolean isToBeTriggered(Boolean decisionBase) {
                return
                    // null check
                    decisionBase != null
                    // condition check
                    && !lastDecisionBases.isEmpty() && lastDecisionBases.get(0) != decisionBase;
            }
        };
    }
    
    /**
     * This method triggers always
     * @return always true
     */
    public static TriggerStrategy always(){
        return values -> true;
    }
}
