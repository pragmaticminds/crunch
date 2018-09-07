package org.pragmaticminds.crunch.api.trigger.comparator;

import java.util.Date;

/**
 * This class holds factories for {@link Supplier} implementations to extract a channel value from a TypedValue
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 13.08.2018
 */
public class Suppliers {
    private Suppliers() {
        throw new UnsupportedOperationException("this constructor should not be used!");
    }
    
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
         * @return a {@link Supplier} that combines both supplier values, null if one of the {@link Supplier}s
         * delivered null.
         */
        public static Supplier<Boolean> and(Supplier<Boolean> s1, Supplier<Boolean> s2){
            String identifier = String.format("and(%s,%s)", s1.getIdentifier(), s2.getIdentifier());
            return new NamedSupplier<>(
                identifier,
                values ->
                    // null check
                    s1.extract(values) != null && s2.extract(values) != null
                    // "and" condition
                    ? s1.extract(values) && s2.extract(values)
                    // return null if one of the suppliers delivered null
                    : null
            );
        }
    
        /**
         * Creates an OR Operation on two {@link Supplier}s
         * @param s1 the first {@link Supplier}
         * @param s2 the second {@link Supplier}
         * @return a {@link Supplier} that combines both supplier values, null if one of the {@link Supplier}s
         * delivered null.
         */
        public static Supplier<Boolean> or(Supplier<Boolean> s1, Supplier<Boolean> s2){
            String identifier = String.format("or(%s,%s)", s1.getIdentifier(), s2.getIdentifier());
            return new NamedSupplier<>(
                identifier,
                values ->
                    // null check
                    s1.extract(values) != null && s2.extract(values) != null
                    // "or" condition
                    ? (s1.extract(values) || s2.extract(values))
                    // return null if one of the suppliers delivered null
                    : null
            );
        }
    
        /**
         * Craetes an inversion of the {@link Supplier} value
         * @param supplier with the result to be inverted
         * @return the inverted result of the inner {@link Supplier} or null if inner {@link Supplier} delivered null.
         */
        public static Supplier<Boolean> not(Supplier<Boolean> supplier){
            String identifier = String.format("not(%s)", supplier.getIdentifier());
            return new NamedSupplier<>(
                identifier,
                values ->
                    // null check
                    supplier.extract(values) != null
                    // "invert" the extracted value
                    ? !supplier.extract(values)
                    // return null of delivered value is null
                    : null
            );
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
            return new NamedSupplier<>(
                identifier,
                values ->
                    // null check
                    supplier.extract(values) != null
                    // compare expected with delivered value
                    ? expected.equals(supplier.extract(values))
                    // return null if delivered value is null
                    : null
            );
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
            return new NamedSupplier<>(
                identifier,
                values ->
                    // null check
                    s1.extract(values) != null && s2.extract(values) != null
                    // compare supplied values with each other
                    ? s1.extract(values).equals(s2.extract(values))
                    // return null if one of the supplied values is null
                    : null
            );
        }
    
        /**
         * Compares a {@link Supplier} value against a regex value for matching
         * @param regex regular expression style {@link String}
         * @param supplier delivers values for matching
         * @return A {@link Supplier} that is matching values
         */
        public static Supplier<Boolean> match(String regex, Supplier<String> supplier){
            String identifier = String.format("match(\"%s\",%s)", regex, supplier.getIdentifier());
            return new NamedSupplier<>(
                identifier,
                values ->
                    // null check
                    supplier.extract(values) != null
                    // match the supplied value to regex
                    ? supplier.extract(values).matches(regex)
                    // if supplied value was null return null
                    : null
            );
        }
    
        /**
         * Checks if the String value of a {@link Supplier} contains the given string value
         * @param string that should be inside the {@link Supplier} string
         * @param supplier delivers values for checking
         * @return A {@link Supplier} that is checked for matching on its values
         */
        public static Supplier<Boolean> contains(String string, Supplier<String> supplier){
            String identifier = String.format("contains(\"%s\",%s)", string, supplier.getIdentifier());
            return new NamedSupplier<>(
                identifier,
                values ->
                    // null check
                    supplier.extract(values) != null
                    // check if supplied value contains the string given
                    ? supplier.extract(values).contains(string)
                    // return null if supplied value was null
                    : null
            );
        }
    
        /**
         * Delivers the length of a {@link Supplier} value {@link String}
         * @param supplier delivers values to extract the length
         * @return A {@link Supplier} that is extracting the length of the {@link String}s
         */
        public static Supplier<Long> length(Supplier<String> supplier){
            String identifier = String.format("length(%s)", supplier.getIdentifier());
            return new NamedSupplier<>(
                identifier,
                values ->
                    // null check
                {
                    String extract = supplier.extract(values);
                    // extract length
                    // return null if supplied value was null
                    if (extract != null) {
                        return (long) extract.length();
                    } else {
                        return null;
                    }
                }
            );
        }
    }
    
}
