package org.pragmaticminds.crunch.api.windowed.extractor.aggregate;

/**
 * Aggregates all values to calculate am avg value as {@link Double}
 * @param <T> type of the incoming values
 *
 * @author Erwin Wagasow
 * craeted by Erwin Wagasow on 23.08.2018
 */
class Avg<T extends Number> implements Aggregation<T>{
    private Double sum;
    private int count = 0;

    /**
     * @return the default identifier of this class
     */
    @Override
    public String getIdentifier() {
        return "avg";
    }

    /**
     * collects one value, all values are passed to this method to be aggregated
     * @param value to be aggregated.
     */
    @Override
    public void aggregate(T value) {
        // ignore null values
        if(value == null){
            return;
        }
        count++;
        if(sum == null){
            sum = value.doubleValue();
        }else{
            sum = AggregationUtils.add(sum, value);
        }
    }

    /**
     * @return the Aggregation result
     */
    @Override
    public Double getAggregated() {
        return AggregationUtils.divide(sum, count);
    }

    /**
     * resets this structure
     */
    @Override
    public void reset() {
        sum = null;
        count = 0;
    }
}
