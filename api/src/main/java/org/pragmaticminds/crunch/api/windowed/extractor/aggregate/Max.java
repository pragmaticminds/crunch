package org.pragmaticminds.crunch.api.windowed.extractor.aggregate;

import java.io.Serializable;

/**
 * Aggregates a maximal value.
 * Should not be directly visible.
 * @param <T> should be something {@link Comparable}
 *
 * @author Erwin Wagasow
 * craeted by Erwin Wagasow on 23.08.2018
 */
class Max<T extends Serializable & Comparable> implements Aggregation<T, T>{
    private T maxValue;

    /**
     * @return default identifier
     */
    @Override
    public String getIdentifier() {
        return "max";
    }

    /**
     * compares if value is bigger than the last one
     * @param value to be aggregated.
     */
    @Override
    public void aggregate(T value) {
        if(maxValue == null || AggregationUtils.compare(value, maxValue) > 0){
            maxValue = value;
        }
    }

    /**
     * @return the maximum value so far
     */
    @Override
    public T getAggregated() {
        return maxValue;
    }

    /**
     * unsets the maximum value so far
     */
    @Override
    public void reset() {
        maxValue = null;
    }
}
