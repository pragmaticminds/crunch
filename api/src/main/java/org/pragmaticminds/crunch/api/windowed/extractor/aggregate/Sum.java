package org.pragmaticminds.crunch.api.windowed.extractor.aggregate;

/**
 * Aggregates the sum of all aggregated values
 * @param <T> type of values
 *
 * @author Erwin Wagasow
 * craeted by Erwin Wagasow on 23.08.2018
 */
class Sum<T extends Number> implements Aggregation<T> {
    protected Double sumValue;

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
            sumValue = value.doubleValue();
        }else{
            sumValue = AggregationUtils.add(sumValue, value);
        }
    }

    /** @return the Aggregation result */
    @Override
    public Double getAggregated() {
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
