package org.pragmaticminds.crunch.api.trigger.comparator;

import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.TypedValues;

import java.io.Serializable;

/**
 * Compares the incoming {@link TypedValues} to internal defined criteria and returns a simplified decision base for
 * the TriggerStrategy, to make a decision if further processing is required.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.07.2018
 */
@FunctionalInterface
public interface Supplier<T> extends Serializable {
    
    /**
     * Compares the incoming values with internal criteria and returns a result of T
     * @param values incoming values to be compared to internal criteria
     * @return a result of T
     */
    T extract(MRecord values);
}
