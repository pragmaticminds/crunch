package org.pragmaticminds.crunch.api.windowed.extractor.aggregate;

/**
 * Aggregates a minimum value
 * @param <T> type of value
 *
 * @author Erwin Wagasow
 * craeted by Erwin Wagasow on 23.08.2018
 */
class Min<T extends Number & Comparable> implements Aggregation<T> {
    private Double minValue;

    /**
     * @return default identifier
     */
    @Override
    public String getIdentifier() {
        return "min";
    }

    /**
     * Compares the current minimum value the given value
     * @param value to be aggregated.
     */
    @Override
    public void aggregate(T value) {
        if(value != null && (minValue == null || AggregationUtils.compare(value, minValue) < 0)){
            minValue = value.doubleValue();
        }
    }

    /**
     * @return the minimum value so far
     */
    @Override
    public Double getAggregated() {
        return minValue;
    }

    /**
     * sets the minimum value so far to null
     */
    @Override
    public void reset() {
        minValue = null;
    }
}
