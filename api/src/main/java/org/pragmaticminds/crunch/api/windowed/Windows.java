package org.pragmaticminds.crunch.api.windowed;

import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;

/**
 * This is a collection of {@link RecordWindow} implementations for usual use cases
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
public class Windows {
    private Windows() { /* hide constructor */ }
    
    /**
     * Window is open as long a supplied {@link Boolean} value is true.
     *
     * @param supplier delivers a {@link Boolean} value.
     * @return a RecordWindow that determines if a window is open.
     */
    public static RecordWindow bitActive(Supplier<Boolean> supplier) {
        return values -> supplier.extract(values) != null && supplier.extract(values);
    }
    
    
    /**
     * Window is open as long a supplied {@link Boolean} value is false.
     *
     * @param supplier delivers a {@link Boolean} value.
     * @return a RecordWindow that determines if a window is open.
     */
    public static RecordWindow bitNotActive(Supplier<Boolean> supplier) {
        return record -> supplier.extract(record) != null && !supplier.extract(record);
    }
    
    /**
     * Window is open as long a supplied value has the expected value.
     *
     * @param supplier delivers a value.
     * @return a RecordWindow that determines if a window is open.
     */
    public static <T> RecordWindow valueEquals(Supplier<T> supplier, T expected) {
        return record -> supplier.extract(record) != null && supplier.extract(record).equals(expected);
    }
    
    /**
     * Window is open as long a supplied value has not the notExpected value.
     *
     * @param supplier delivers a value.
     * @return a RecordWindow that determines if a window is open.
     */
    public static <T> RecordWindow valueNotEquals(Supplier<T> supplier, T notExpected) {
        return record -> supplier.extract(record) != null && !supplier.extract(record).equals(notExpected);
    }
}