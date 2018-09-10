package org.pragmaticminds.crunch.api.trigger.strategy;

import org.pragmaticminds.crunch.api.records.MRecord;
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
    public static <T> TriggerStrategy onChange(Supplier<T> supplier){
        return new MemoryTriggerStrategy<T>(supplier, 1) {
            /**
             * This method is to be implemented by the user of this class, with the final decision making.
             *
             * @param decisionBase the extracted value
             * @return true if triggered, otherwise false
             */
            @Override
            public boolean isToBeTriggered(T decisionBase) {
                return
                    // null check
                    decisionBase != null
                    // condition check
                    && !lastDecisionBases.isEmpty() && !lastDecisionBases.get(0).equals(decisionBase);
            }
        };
    }

    /**
     * This method triggers on supplied value is not available
     * @param supplier extracts the relevant values from a {@link MRecord}
     * @param <T> type of the Supplier
     * @return true if triggered
     */
    public static <T> TriggerStrategy onNull(Supplier<T> supplier){
        return values -> supplier.extract(values) == null;
    }

    /**
     * This method triggers on supplied value is available
     * @param supplier extracts the relevant values from a {@link MRecord}
     * @param <T> type of the Supplier
     * @return true if triggered
     */
    public static <T> TriggerStrategy onNotNull(Supplier<T> supplier){
        return values -> supplier.extract(values) != null;
    }

    /**
     * This method triggers always
     * @return always true
     */
    public static TriggerStrategy always(){
        return values -> true;
    }
}
