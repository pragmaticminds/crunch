package org.pragmaticminds.crunch.api.trigger.filter;

import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.trigger.TriggerEvaluationFunction;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

/**
 * This filter interface is used to filter out Events before they are given to the out collector in the
 * {@link TriggerEvaluationFunction}.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 14.08.2018
 */
public interface EventFilter<T extends Serializable> extends Serializable {
    
    /**
     * Checks if a filtration is to be applied to the given parameters
     * @param event the extracted from the processing
     * @param values the processed values
     * @return true if filter is to be applied, else false
     */
    boolean apply(T event, MRecord values);
    
    /**
     * Collects all channel identifiers that are used to filter.
     *
     * @return a {@link List} or {@link Collection} of all used channel identifiers.
     */
    Collection<String> getChannelIdentifiers();
}
