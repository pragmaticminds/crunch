package org.pragmaticminds.crunch.api.windowed.extractor.aggregate;

import java.io.Serializable;

/**
 * This is a collection of {@link Aggregation} implementations for usual use cases.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public class Aggregations implements Serializable {
    private Aggregations() { /* hide the constructor */ }
    
    /**
     * Creates an implementation of {@link Aggregation} that searches for the biggest value in the aggregated values.
     * @param <T> type of the values
     * @return the biggest value
     */
    public static <T extends Serializable & Comparable> Aggregation<T, T> max(){
        return new Max<>();
    }
    
    /**
     * Creates an implementation of {@link Aggregation} that searches for the smallest value in the aggregated values.
     * @param <T> type of the values
     * @return the smallest value
     */
    public static <T extends Serializable & Comparable> Aggregation<T, T> min(){
        return new Min<>();
    }
    
    /**
     * Creates an implementation of {@link Aggregation} that sums up all aggregated values.
     * @param <T> type of the values
     * @return the sum of all aggregated values
     */
    public static <T extends Serializable> Aggregation<T, T> sum(){
        return new Sum<>();
    }
    
    /**
     * Creates an implementation of {@link Aggregation} that calculates the avg value of accumulated values
     * @param <T> type of the values
     * @return the calculated avg value from aggregated values
     */
    public static <T extends Serializable> Aggregation<T, Double> avg(){
        return new Avg<>();
    }
}
