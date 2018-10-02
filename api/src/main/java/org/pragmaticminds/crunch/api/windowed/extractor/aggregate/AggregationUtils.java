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
     *
     * @param o1 the first candidate for comparison
     * @param o2 the second candidate for comparison
     * @param <T> is the type of both comparison candidates
     * @return 0 if both are equal, a positive int value if o1 is bigger than o2 and a negative int value otherwise
     */
    @SuppressWarnings("unchecked") // both types are the same and extends from Comparable
    public static <T extends Comparable, I extends Comparable> int compare(T o1, I o2){
        if(o1.getClass() == o2.getClass()){
            return o1.compareTo(o2);
        }else if(Number.class.isAssignableFrom(o1.getClass()) && Number.class.isAssignableFrom(o2.getClass())) {
            return Double.compare(((Number) o1).doubleValue(), ((Number) o2).doubleValue());
        }else if(String.class.isAssignableFrom(o1.getClass()) || String.class.isAssignableFrom(o2.getClass())){
            return o1.toString().compareTo(o2.toString());
        }else{
            throw new UnsupportedOperationException(
                String.format("Could not compare %s with %s!",o1.getClass(),o2.getClass())
            );
        }
    }
    
    /**
     * This method sums up common number values.
     * @param o1 first operand
     * @param o2 second operand
     * @param <T> type of operand 1
     * @param <I> type of operand 2
     * @return the sum of both addends in their common type
     */
    @SuppressWarnings("unchecked") // manually checked type security
    public static <T extends Number, I extends Number> Double add(T o1, I o2){
        if(o1 == null || o2 == null){
            return null;
        }
        return o1.doubleValue() + o2.doubleValue();
    }
    
    /**
     * This method subtracts common number values.
     * @param o1 the one to be subtracted from
     * @param o2 the one that is subtracted from o1
     * @param <T> type of o1
     * @param <I> type of o2
     * @return the subtraction of o2 from o1 in their common type
     */
    @SuppressWarnings("unchecked") // manually checked type security
    public static <T extends Number, I extends Number> Double subtract(T o1, I o2){
        if(o1 == null || o2 == null){
            return null;
        }
        return o1.doubleValue() - o2.doubleValue();
    }
    
    /**
     * This method multiplies two common number values.
     * @param m1 first operand
     * @param m1 second operand
     * @param <T> type of m1
     * @param <I> type of m2
     * @return the result of the multiplication
     */
    @SuppressWarnings("unchecked") // manually checked type security
    public static <T extends Number, I extends Number> Double multiply(T m1, I m2){
        if(m1 == null || m2 == null){
            return null;
        }
        return m1.doubleValue() * m2.doubleValue();
    }
    
    /**
     * This method divides two common number values.
     * @param divided the divided
     * @param divider the divider
     * @param <T> type of divided
     * @param <I> type of divider
     * @return the result of the division
     */
    @SuppressWarnings("unchecked") // manually checked type security
    public static <T extends Number, I extends Number> Double divide(T divided, I divider){
        if(divided == null || divider == null){
            return null;
        }
        return divided.doubleValue()/divider.doubleValue();
    }
}
