package org.pragmaticminds.crunch.api.windowed.extractor.aggregate;

import java.io.Serializable;

/**
 * This abstract class accumulates all given values and returns am aggregated value, when called.
 * It also has an identifier, which can be read or set.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public interface Aggregation<T extends Serializable> extends Serializable {
    /**
     * getter for identifier
     * @return the identifier of this implementation
     */
    String getIdentifier();
    
    /**
     * Takes the given value and stores it until the getAggregated method is called.
     * @param value to be aggregated.
     */
    public void aggregate(T value);
    
    /** @return the aggregated value of all values that have been aggregated by the aggregate method. */
    public abstract Double getAggregated();
    
    /** cleans up the accumulated values */
    public void reset();
}
