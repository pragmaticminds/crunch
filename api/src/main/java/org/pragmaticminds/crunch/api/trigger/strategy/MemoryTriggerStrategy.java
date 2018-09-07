package org.pragmaticminds.crunch.api.trigger.strategy;

import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;
import org.pragmaticminds.crunch.api.values.TypedValues;

import java.util.ArrayList;

/**
 * This is a helper class for the {@link TriggerStrategies} class and other implementations.
 * It prevents duplicate code. It memorizes the last values of the decision base.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 13.08.2018
 */
public abstract class MemoryTriggerStrategy<T> implements TriggerStrategy {
    
    protected ArrayList<T> lastDecisionBases = new ArrayList<>();
    protected Supplier<T>  supplier;
    protected int          bufferSize;
    
    /**
     * Main constructor
     * @param supplier of the decision base, which extracts relevant values from the {@link TypedValues}
     * @param bufferSize defines how many steps are to be safed
     */
    public MemoryTriggerStrategy(Supplier<T> supplier, int bufferSize) {
        this.supplier = supplier;
        this.bufferSize = bufferSize;
    }
    
    /**
     * This method decides whether it is to be triggered or not.
     * This method is implemented in here an leads out the part of decision making into the method
     * isToBeTriggered(Boolean decisionBase).
     * @param values for the {@link Supplier} to extract the relevant data
     * @return true if triggered, otherwise false
     */
    @Override
    public boolean isToBeTriggered(MRecord values) {
        T decisionBase = supplier.extract(values);
        
        // ignore null values
        if(decisionBase == null){
            return false;
        }
        
        boolean result = isToBeTriggered(decisionBase);
        lastDecisionBases.add(decisionBase);
        if(lastDecisionBases.size() > bufferSize){
            lastDecisionBases.remove(0);
        }
        return result;
    }
    
    /**
     * This method is to be implemented by the user of this class, with the final decision making.
     * @param decisionBase the extracted value
     * @return true if triggered, otherwise false
     */
    public abstract boolean isToBeTriggered(T decisionBase);
}
