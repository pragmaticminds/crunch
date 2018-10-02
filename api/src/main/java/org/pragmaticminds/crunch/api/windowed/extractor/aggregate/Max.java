package org.pragmaticminds.crunch.api.windowed.extractor.aggregate;

/**
 * Aggregates a maximal value.
 * Should not be directly visible.
 * @param <T> should be something {@link Comparable}
 *
 * @author Erwin Wagasow
 * craeted by Erwin Wagasow on 23.08.2018
 */
class Max<T extends Number & Comparable> implements Aggregation<T>{
    private Double maxValue;

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
        if(value == null){
            return;
        }
        double innerValue = value.doubleValue();
        if(maxValue == null || AggregationUtils.compare(innerValue, maxValue) > 0){
            maxValue = value.doubleValue();
        }
    }

    /**
     * @return the maximum value so far
     */
    @Override
    public Double getAggregated() {
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
