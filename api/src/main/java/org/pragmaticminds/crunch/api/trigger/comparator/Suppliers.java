package org.pragmaticminds.crunch.api.trigger.comparator;

import org.pragmaticminds.crunch.api.windowed.extractor.aggregate.AggregationUtils;

import java.util.Date;

/**
 * This class holds factories for {@link Supplier} implementations to extract a channel value from a TypedValue
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 13.08.2018
 */
public class Suppliers {
    private Suppliers() { /* do nothing */}
    
    /**
     * Holds all the channel extraction {@link Supplier}s
     */
    public static class ChannelExtractors {
        private ChannelExtractors(){ /* do nothing */}
        
        /**
         * extracts a {@link Boolean} value from the values
         *
         * @param name of the channel
         * @return the value of the channel
         */
        public static Supplier<Boolean> booleanChannel(String name) {
            return new NamedSupplier<>(name, values -> values.getBoolean(name));
        }
        
        /**
         * extracts a {@link Double} value from the values
         *
         * @param name of the channel
         * @return the value of the channel
         */
        public static Supplier<Double> doubleChannel(String name) {
            return new NamedSupplier<>(name, values -> values.getDouble(name));
        }
        
        /**
         * extracts a {@link Long} value from the values
         *
         * @param name of the channel
         * @return the value of the channel
         */
        public static Supplier<Long> longChannel(String name) {
            return new NamedSupplier<>(name, values -> values.getLong(name));
        }
        
        /**
         * extracts a {@link Date} value from the values
         *
         * @param name of the channel
         * @return the value of the channel
         */
        public static Supplier<Date> dateChannel(String name) {
            return new NamedSupplier<>(name, values -> values.getDate(name));
        }
        
        /**
         * extracts a {@link String} value from the values
         *
         * @param name of the channel
         * @return the value of the channel
         */
        public static Supplier<String> stringChannel(String name) {
            return new NamedSupplier<>(name, values -> values.getString(name));
        }
    }
    
    /**
     * Holds all boolean combination operator {@link Supplier}s
     */
    public static class BooleanOperators{
        private BooleanOperators() { /* do nothing */ }
    
        /**
         * Creates an AND Operation on two {@link Supplier}s
         * @param s1 the first {@link Supplier}
         * @param s2 the second {@link Supplier}
         * @return a {@link Supplier} that combines both supplier values
         */
        public static Supplier<Boolean> and(Supplier<Boolean> s1, Supplier<Boolean> s2){
            String identifier = String.format("and(%s,%s)", s1.getIdentifier(), s2.getIdentifier());
            return new NamedSupplier<>(identifier, values -> s1.extract(values) && s2.extract(values));
        }
    
        /**
         * Creates an OR Operation on two {@link Supplier}s
         * @param s1 the first {@link Supplier}
         * @param s2 the second {@link Supplier}
         * @return a {@link Supplier} that combines both supplier values
         */
        public static Supplier<Boolean> or(Supplier<Boolean> s1, Supplier<Boolean> s2){
            String identifier = String.format("or(%s,%s)", s1.getIdentifier(), s2.getIdentifier());
            return new NamedSupplier<>(identifier, values -> s1.extract(values) || s2.extract(values));
        }
    
        /**
         * Craetes an inversion of the {@link Supplier} value
         * @param supplier with the result to be inverted
         * @return the inverted result of the inner {@link Supplier}
         */
        public static Supplier<Boolean> not(Supplier<Boolean> supplier){
            String identifier = String.format("not(%s)", supplier.getIdentifier());
            return new NamedSupplier<>(identifier, values -> !supplier.extract(values));
        }
    }
    
    /**
     * Holds all {@link String} operator {@link Supplier}s
     */
    public static class StringOperators{
        private StringOperators() { /* do nothing */ }
    
        /**
         * Compares the expected string with the extracted value of the {@link Supplier}
         * @param expected {@link String} value
         * @param supplier delivers the comparison value
         * @return A {@link Supplier} that compares equality
         */
        @SuppressWarnings("squid:S1221") // using of the name equal
        public static Supplier<Boolean> equal(String expected, Supplier<String> supplier){
            String identifier = String.format("equal(\"%s\",%s)", expected, supplier.getIdentifier());
            return new NamedSupplier<>(identifier, values -> expected.equals(supplier.extract(values)));
        }
    
        /**
         * Compares {@link String} values from two {@link Supplier}s
         * @param s1 first {@link Supplier} that delivers values for comparison
         * @param s2 second {@link Supplier} that delivers values for comparison
         * @return A {@link Supplier} that compares equality if both {@link Supplier} values
         */
        @SuppressWarnings("squid:S1221") // using of the name equal
        public static Supplier<Boolean> equal(Supplier<String> s1, Supplier<String> s2){
            String identifier = String.format("equal(%s,%s)", s1.getIdentifier(), s2.getIdentifier());
            return new NamedSupplier<>(identifier, values -> s1.extract(values).equals(s2.extract(values)));
        }
    
        /**
         * Compares a {@link Supplier} value against a regex value for matching
         * @param regex regular expression style {@link String}
         * @param supplier delivers values for matching
         * @return A {@link Supplier} that is matching values
         */
        public static Supplier<Boolean> match(String regex, Supplier<String> supplier){
            String identifier = String.format("match(\"%s\",%s)", regex, supplier.getIdentifier());
            return new NamedSupplier<>(identifier, values -> supplier.extract(values).matches(regex));
        }
    
        /**
         * Checks if the String value of a {@link Supplier} contains the given string value
         * @param string that should be inside the {@link Supplier} string
         * @param supplier delivers values for checking
         * @return A {@link Supplier} that is checked for matching on its values
         */
        public static Supplier<Boolean> contains(String string, Supplier<String> supplier){
            String identifier = String.format("contains(\"%s\",%s)", string, supplier.getIdentifier());
            return new NamedSupplier<>(identifier, values -> supplier.extract(values).contains(string));
        }
    
        /**
         * Delivers the length of a {@link Supplier} value {@link String}
         * @param supplier delivers values to extract the length
         * @return A {@link Supplier} that is extracting the length of the {@link String}s
         */
        public static Supplier<Long> length(Supplier<String> supplier){
            String identifier = String.format("length(%s)", supplier.getIdentifier());
            return new NamedSupplier<>(identifier, values -> (long) supplier.extract(values).length());
        }
    }
    
    /**
     * Holds comparative {@link Supplier}s
     */
    public static class Comparators{
        /** hidden constructor */
        private Comparators() { throw new UnsupportedOperationException("this class should not be instantiated!"); }
    
        /**
         * Compares given value with the supplied value for equality.
         *
         * @param value to be compared.
         * @param supplier extracts the values to be compared
         * @param <T> type of values to be compared
         * @return an equality check {@link Supplier}
         */
        @SuppressWarnings("squid:S1201") // use of equals
        public static <T> Supplier<Boolean> equals(T value, Supplier<T> supplier){
            String identifier = String.format("equals(%s, %s)", value.toString(), supplier.getIdentifier());
            return new NamedSupplier<>(identifier, values ->
                // null check
                supplier.extract(values) != null
                // apply condition
                && value.equals(supplier.extract(values))
            );
        }
        
        /**
         * Compares two supplied values for equality.
         *
         * @param s1 supplies the first value for comparison
         * @param s2 supplies the second value for comparison
         * @param <T> type of the values to be compared
         * @return an equality check {@link Supplier}
         */
        @SuppressWarnings("squid:S1201") // use of equals
        public static <T> Supplier<Boolean> equals(Supplier<T> s1, Supplier<T> s2){
            String identifier = String.format("equals(%s, %s)", s1.getIdentifier(), s2.getIdentifier());
            return new NamedSupplier<>(identifier, values ->
                // check for null values
                s1.extract(values) != null && s2.extract(values) != null
                // check the condition
                && s1.extract(values).equals(s2.extract(values)));
        }
    
        /**
         * Compares two supplied values and return the difference as a Long.
         * Determines a difference value for the two supplied values. If the values are equal the result is 0, if value
         * of s1 is bigger, than the resulting number is >1, if value of s2 is bigger, than the resulting value is
         * negative.
         *
         * @param s1 supplies the fist value for comparison
         * @param s2 supplies the second value for comparison
         * @param <T> type of compared values
         * @return a difference comparison {@link Supplier}
         */
        public static <T extends Comparable> Supplier<Long> compare(Supplier<T> s1, Supplier<T> s2){
            String identifier = String.format("compare(%s, %s)", s1.getIdentifier(), s2.getIdentifier());
            return new NamedSupplier<>(identifier, values -> {
                // extract values
                T v1 = s1.extract(values);
                T v2 = s2.extract(values);
                // null check
                if(v1 == null || v2 == null){
                    return null;
                }
                // apply comparison
                return (long)AggregationUtils.compare(v1, v2);
            });
        }
    
        /**
         * Compares a value with a supplied value and return the difference as a Long
         * Determines a difference value for the value and the supplied value. If the values are equal the result is 0,
         * if value of s1 is bigger, than the resulting number is >1, if value of s2 is bigger, than the resulting value
         * is negative.
         *
         * @param value the fix value for comparison
         * @param supplier supplies the second value for comparison
         * @param <T> type of compared values
         * @return a difference comparison {@link Supplier}
         */
        public static <T extends Comparable> Supplier<Long> compare(T value, Supplier<T> supplier){
            String identifier = String.format("compare(%s, %s)", value.toString(), supplier.getIdentifier());
            return new NamedSupplier<>(identifier, values -> {
                // extract value
                T value2 = supplier.extract(values);
                // null check
                if (value2 == null) {
                    return null;
                }
                // apply comparison
                return (long) AggregationUtils.compare(value, value2);
            });
        }
    }
    
}
