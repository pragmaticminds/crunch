package org.pragmaticminds.crunch.api.trigger.strategy;

import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;
import org.pragmaticminds.crunch.api.values.TypedValues;

import java.io.Serializable;
import java.util.HashSet;


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
        return new LambdaTriggerStrategy(
            values -> supplier.extract(values) != null && supplier.extract(values),
            () -> new HashSet<>(supplier.getChannelIdentifiers())
        );
    }
    
    /**
     * This method triggers always on supplied value = false
     * @param supplier extracts the relevant values from a {@link TypedValues}
     * @return true if triggered, false if not or supplied value is null
     */
    public static TriggerStrategy onFalse(Supplier<Boolean> supplier){
        return new LambdaTriggerStrategy(
            values ->
                // null check
                supplier.extract(values) != null
                // on false check
                && !supplier.extract(values),
            () -> new HashSet<>(supplier.getChannelIdentifiers())
        );
    }
    
    /**
     * This method triggers on supplied value = true and the last supplied value = false
     * @param supplier extracts the relevant values from a {@link TypedValues}
     * @return true if triggered
     */
    public static TriggerStrategy onBecomeTrue(Supplier<Boolean> supplier){
        return onBecomeTrue(supplier, null);
    }
    
    /**
     * This method triggers on supplied value = true and the last supplied value = false
     * @param supplier extracts the relevant values from a {@link TypedValues}
     * @param initialValue the first value to compare with
     * @return true if triggered
     */
    public static TriggerStrategy onBecomeTrue(Supplier<Boolean> supplier, Boolean initialValue){
        return new MemoryTriggerStrategy<Boolean>(supplier, 1, initialValue) {
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
        return onBecomeFalse(supplier, null);
    }
    
    /**
     * This method triggers on supplied value = false and the last supplied value = true
     * @param supplier extracts the relevant values from a {@link TypedValues}
     * @param initialValue the first value to compare with
     * @return true if triggered
     */
    public static TriggerStrategy onBecomeFalse(Supplier<Boolean> supplier, Boolean initialValue){
        return new MemoryTriggerStrategy<Boolean>(supplier, 1, initialValue) {
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
    public static <T extends Serializable> TriggerStrategy onChange(Supplier<T> supplier){
        return onChange(supplier, null);
    }
    
    /**
     * This method triggers on supplied value != last supplied value
     * @param supplier extracts the relevant values from a {@link TypedValues}
     * @param initialValue the first value to compare with
     * @return true if triggered
     */
    public static <T extends Serializable> TriggerStrategy onChange(Supplier<T> supplier, T initialValue){
        return new MemoryTriggerStrategy<T>(supplier, 1, initialValue) {
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
        return new LambdaTriggerStrategy(
            values -> supplier.extract(values) == null,
            () -> new HashSet<>(supplier.getChannelIdentifiers())
        );
    }

    /**
     * This method triggers on supplied value is available
     * @param supplier extracts the relevant values from a {@link MRecord}
     * @param <T> type of the Supplier
     * @return true if triggered
     */
    public static <T> TriggerStrategy onNotNull(Supplier<T> supplier){
        return new LambdaTriggerStrategy(
            values -> supplier.extract(values) != null,
            () -> new HashSet<>(supplier.getChannelIdentifiers())
        );
    }

    /**
     * This method triggers always
     * @return always true
     */
    public static TriggerStrategy always(){
        return new LambdaTriggerStrategy(
            values -> true, HashSet::new
        );
    }
}
