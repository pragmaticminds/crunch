package org.pragmaticminds.crunch.api.windowed.extractor.aggregate;

import java.io.Serializable;

/**
 * This class supplies common operations for common value types
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public class AggregationUtils implements Serializable {
    private AggregationUtils(){ /* hide constructor */}
    
    /**
     * Compares most common types with a int value as difference result
     * @param o1 the first candidate for comparison
     * @param o2 the second candidate for comparison
     * @param <T> is the type of both comparison candidates
     * @return 0 if both are equal, a positive int value if o1 is bigger than o2 and a negative int value otherwise
     */
    @SuppressWarnings("unchecked") // both types are the same and extends from Comparable
    public static <T extends Comparable> int compare(T o1, T o2){
        return o1.compareTo(o2);
    }
    
    /**
     * This method sums up common number values.
     * @param o1 first addend
     * @param o2 second addend
     * @param <T> type of both addends
     * @return the sum of both addends in their common type
     */
    @SuppressWarnings("unchecked") // manually checked type security
    public static <T> T sum(T o1, T o2){
        // Integer
        if(o1.getClass().isAssignableFrom(Integer.class)){
            return (T) Integer.valueOf(((Integer)o1) + ((Integer)o2));
        // Long
        } else if(o1.getClass().isAssignableFrom(Long.class)){
            return (T) Long.valueOf(((Long)o1) + ((Long)o2));
        // Float
        } else if(o1.getClass().isAssignableFrom(Float.class)){
            return (T) Float.valueOf(((Float)o1) + ((Float)o2));
        // Double
        } else if(o1.getClass().isAssignableFrom(Double.class)){
            return (T) Double.valueOf(((Double)o1) + ((Double)o2));
        // Error unknown type -> throw exception
        } else {
            throw new UnsupportedOperationException();
        }
    }
    
    /**
     * This method divides two common number values.
     * @param divided the divided
     * @param divider the divider
     * @param <T> type of both parameters
     * @return the result of the devision
     */
    @SuppressWarnings("unchecked") // manually checked type security
    public static <T> Double divide(T divided, int divider){
        // Integer
        if(divided.getClass().isAssignableFrom(Integer.class)){
            return ((Integer) divided) / (double) divider;
        // Long
        } else if(divided.getClass().isAssignableFrom(Long.class)){
            return ((Long)divided) / (double) divider;
        // Float
        } else if(divided.getClass().isAssignableFrom(Float.class)){
            return ((Float)divided) / (double) divider;
        // Double
        } else if(divided.getClass().isAssignableFrom(Double.class)){
            return ((Double)divided) / divider;
        // Error unknown type -> throw exception
        } else {
            throw new UnsupportedOperationException();
        }
    }
}
