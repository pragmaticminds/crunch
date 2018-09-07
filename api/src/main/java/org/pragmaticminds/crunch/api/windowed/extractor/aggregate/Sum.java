package org.pragmaticminds.crunch.api.windowed.extractor.aggregate;

import java.io.Serializable;

/**
 * Aggregates the sum of all aggregated values
 * @param <T> type of values
 *
 * @author Erwin Wagasow
 * craeted by Erwin Wagasow on 23.08.2018
 */
class Sum<T extends Serializable> implements Aggregation<T, T> {
    protected T sumValue;

    /**
     * @return default identifier
     */
    @Override
    public String getIdentifier() {
        return "sum";
    }

    /**
     * @param value to be aggregated.
     */
    @Override
    public void aggregate(T value) {
        if(value == null){
            return;
        }
        if(sumValue == null){
            sumValue = value;
        }else{
            sumValue = AggregationUtils.sum(sumValue, value);
        }
    }

    /** @return the Aggregation result */
    @Override
    public T getAggregated() {
        return sumValue;
    }

    /**
     * resets the structures of this class
     */
    @Override
    public void reset() {
        sumValue = null;
    }
}
