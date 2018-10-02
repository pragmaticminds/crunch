package org.pragmaticminds.crunch.api.trigger.comparator;

import org.pragmaticminds.crunch.api.values.dates.Value;
import org.pragmaticminds.crunch.api.windowed.extractor.aggregate.AggregationUtils;

import java.io.Serializable;
import java.util.*;
import java.util.HashSet;

import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.IdentifierCombiner.combine;

/**
 * This class holds factories for {@link Supplier} implementations to extract a channel value from a TypedValue
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 13.08.2018
 */
public final class Suppliers {
    private Suppliers() {
        throwUnsupportedOperationException();
    }
    
    private static void throwUnsupportedOperationException(){
        throw new UnsupportedOperationException("this should never be initialized!");
    }
    
    /**
     * Holds all the channel extraction {@link Supplier}s
     */
    public static class ChannelExtractors {
        /** hidden constructor */
        private ChannelExtractors(){
            throwUnsupportedOperationException();
        }
        
        /**
         * extracts a {@link Boolean} value from the values
         *
         * @param name of the channel
         * @return the value of the channel
         */
        public static Supplier<Boolean> booleanChannel(String name) {
            return new NamedSupplier<>(
                name,
                values -> values.getBoolean(name),
                    () -> new HashSet<>(Collections.singletonList(name))
            );
        }
        
        /**
         * extracts a {@link Double} value from the values
         *
         * @param name of the channel
         * @return the value of the channel
         */
        public static Supplier<Double> doubleChannel(String name) {
            return new NamedSupplier<>(
                name,
                values -> values.getDouble(name),
                    () -> new HashSet<>(Collections.singletonList(name))
            );
        }
        
        /**
         * extracts a {@link Long} value from the values
         *
         * @param name of the channel
         * @return the value of the channel
         */
        public static Supplier<Long> longChannel(String name) {
            return new NamedSupplier<>(
                name,
                values -> values.getLong(name),
                    () -> new HashSet<>(Collections.singletonList(name))
            );
        }
        
        /**
         * extracts a {@link Date} value from the values
         *
         * @param name of the channel
         * @return the value of the channel
         */
        public static Supplier<Date> dateChannel(String name) {
            return new NamedSupplier<>(
                name,
                values -> values.getDate(name),
                    () -> new HashSet<>(Collections.singletonList(name))
            );
        }
        
        /**
         * extracts a {@link String} value from the values
         *
         * @param name of the channel
         * @return the value of the channel
         */
        public static Supplier<String> stringChannel(String name) {
            return new NamedSupplier<>(
                name,
                values -> values.getString(name),
                    () -> new HashSet<>(Collections.singletonList(name))
            );
        }

        /**
         * Extracts a value from a channel without explicit type knowledge.
         *
         * @param name of the channel
         * @return a {@link Supplier} for the given channel
         */
        public static Supplier<Value> channel(String name) {
            return new NamedSupplier<>(
                name,
                values -> values.getValue(name),
                    () -> new HashSet<>(Collections.singletonList(name))
            );
        }
    }
    
    /**
     * Holds all boolean combination operator {@link Supplier}s
     */
    public static class BooleanOperators{
        /** hidden constructor */
        private BooleanOperators() {
            throwUnsupportedOperationException();
        }
    
        /**
         * Creates an AND Operation on two {@link Supplier}s
         * @param s1 the first {@link Supplier}
         * @param s2 the second {@link Supplier}
         * @return a {@link Supplier} that combines both supplier values, null if one of the {@link Supplier}s
         * delivered null.
         */
        public static Supplier<Boolean> and(Supplier<Boolean> s1, Supplier<Boolean> s2){
            return createSupplier(
                s1,
                s2,
                String.format("and(%s,%s)", s1.getIdentifier(), s2.getIdentifier()),
                (SupplierLambda<Boolean, Boolean, Boolean>) (v1, v2) -> v1 && v2
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
            return createSupplier(
                s1,
                s2,
                String.format("or(%s,%s)", s1.getIdentifier(), s2.getIdentifier()),
                (SupplierLambda<Boolean, Boolean, Boolean>) (v1, v2) -> v1 || v2
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
                    : null,
                () -> new HashSet<>(supplier.getChannelIdentifiers())
            );
        }
    }
    
    /**
     * Holds all {@link String} operator {@link Supplier}s
     */
    public static class StringOperators{
        /** hidden constructor */
        private StringOperators() {
            throwUnsupportedOperationException();
        }
    
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
                    : null,
                () -> new HashSet<>(supplier.getChannelIdentifiers())
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
                    : null,
                () -> combine(s1, s2)
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
                    : null,
                () -> new HashSet<>(supplier.getChannelIdentifiers())
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
                    : null,
                () -> new HashSet<>(supplier.getChannelIdentifiers())
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
                },
                () -> new HashSet<>(supplier.getChannelIdentifiers())
            );
        }
    }
    
    /**
     * Holds comparative {@link Supplier}s
     */
    public static class Comparators{
        /** hidden constructor */
        private Comparators() {
            throwUnsupportedOperationException();
        }
    
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
                && value.equals(supplier.extract(values)),
                () -> new HashSet<>(supplier.getChannelIdentifiers())
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
        public static <T extends Comparable & Serializable, I extends Comparable & Serializable> Supplier<Boolean> equals(Supplier<T> s1, Supplier<I> s2){
            return createSupplier(
                s1,
                s2,
                String.format("equals(%s, %s)", s1.getIdentifier(), s2.getIdentifier()),
                (ComparableSupplierLambda<T,I,Boolean>) (v1, v2) -> 0 == AggregationUtils.compare(v1, v2)
            );
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
        public static <T extends Comparable & Serializable, I extends Comparable & Serializable> Supplier<Long> compare(Supplier<T> s1, Supplier<I> s2){
            return createSupplier(
                s1,
                s2,
                String.format("compare(%s, %s)", s1.getIdentifier(), s2.getIdentifier()),
                (ComparableSupplierLambda<T,I,Long>) (v1, v2) -> (long)AggregationUtils.compare(v1,v2)
            );
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
        public static <T extends Comparable & Serializable, I extends Comparable & Serializable> Supplier<Long> compare(T value, Supplier<I> supplier){
            return createSupplier(
                value,
                supplier,
                String.format("compare(%s, %s)", value.toString(), supplier.getIdentifier()),
                (v1, v2) -> (long) AggregationUtils.compare(v1, v2)
            );
        }
    
        /**
         * Logical operation: T value &lt; Supplier&lt;I&gt; supplier.
         *
         * @param value    first operand
         * @param supplier second operand supplier
         * @param <T>      type of value
         * @param <I>      inner type of supplier
         * @return {@link Supplier} of {@link Boolean} type
         */
        public static <T extends Number, I extends Number> Supplier<Boolean> lowerThan(T value, Supplier<I> supplier){
            return createSupplier(
                value,
                supplier,
                String.format("lowerThan(%s, %s)", value, supplier.getIdentifier()),
                (v1, v2) -> v1.doubleValue() < v2.doubleValue()
            );
        }
    
        /**
         * Logical operation: s1 &lt; s2
         *
         * @param s1 first operand supplier
         * @param s2 second operand supplier
         * @param <T> inner type of s1
         * @param <I> inner type of s2
         * @return {@link Supplier} of {@link Boolean} type
         */
        public static <T extends Number, I extends Number> Supplier<Boolean> lowerThan(Supplier<T> s1, Supplier<I> s2){
            return createSupplier(
                s1,
                s2,
                String.format("lowerThan(%s, %s)", s1.getIdentifier(), s2.getIdentifier()),
                (NumberSupplierLambda<T,I,Boolean>) (v1, v2) -> v1.doubleValue() < v2.doubleValue()
            );
        }
    
        /**
         * Logical operation: value &lt;= supplier
         *
         * @param value     first operand
         * @param supplier  second operand supplier
         * @param <T>       type of value
         * @param <I>       inner type of supplier
         * @return {@link Supplier} of {@link Boolean} type
         */
        public static <T extends Number, I extends Number> Supplier<Boolean> lowerThanEquals(T value, Supplier<I> supplier) {
            return createSupplier(
                value,
                supplier,
                String.format("lowerThanEquals(%s, %s)", value, supplier.getIdentifier()),
                (v1, v2) -> v1.doubleValue() <= v2.doubleValue()
            );
        }
    
        /**
         * Logical operation: s1 &lt;= s2
         *
         * @param s1 first operand supplier
         * @param s2 second operand supplier
         * @param <T> inner type of s1
         * @param <I> inner type of s2
         * @return {@link Supplier} of {@link Boolean} type
         */
        public static <T extends Number, I extends Number> Supplier<Boolean> lowerThanEquals(Supplier<T> s1, Supplier<I> s2) {
            return createSupplier(
                s1,
                s2,
                String.format("lowerThanEquals(%s, %s)", s1.getIdentifier(), s2.getIdentifier()),
                (NumberSupplierLambda<T,I,Boolean>) (v1, v2) -> v1.doubleValue() <= v2.doubleValue()
            );
        }
    
        /**
         * Logical operation: value &gt; supplier
         *
         * @param value     first operand
         * @param supplier  second operand supplier
         * @param <T>       type of value
         * @param <I>       inner type of supplier
         * @return {@link Supplier} of {@link Boolean} type
         */
        public static <T extends Number, I extends Number> Supplier<Boolean> greaterThan(T value, Supplier<I> supplier) {
            return createSupplier(
                value,
                supplier,
                String.format("greaterThan(%s, %s)", value, supplier.getIdentifier()),
                (v1, v2) -> v1.doubleValue() > v2.doubleValue()
            );
        }
    
        /**
         * Logical operation: s1 &gt; s2
         *
         * @param s1 first operand supplier
         * @param s2 second operand supplier
         * @param <T> inner type of s1
         * @param <I> inner type of s2
         * @return {@link Supplier} of {@link Boolean} type
         */
        public static <T extends Number, I extends Number> Supplier<Boolean> greaterThan(Supplier<T> s1, Supplier<I> s2) {
            return createSupplier(
                s1,
                s2,
                String.format("greaterThan(%s, %s)", s1.getIdentifier(), s2.getIdentifier()),
                (NumberSupplierLambda<T,I,Boolean>) (v1, v2) -> v1.doubleValue() > v2.doubleValue()
            );
        }
    
        /**
         * Logical operation: value &gt;= supplier
         *
         * @param value     first operand
         * @param supplier  second operand supplier
         * @param <T>       type of value
         * @param <I>       inner type of supplier
         * @return {@link Supplier} of {@link Boolean} type
         */
        public static <T extends Number, I extends Number> Supplier<Boolean> greaterThanEquals(T value, Supplier<I> supplier) {
            return createSupplier(
                value,
                supplier,
                String.format("greaterThanEquals(%s, %s)", value, supplier.getIdentifier()),
                (v1, v2) -> v1.doubleValue() >= v2.doubleValue()
            );
        }
    
        /**
         * Logical operation: s1 &gt;= s2
         *
         * @param s1 first operand supplier
         * @param s2 second operand supplier
         * @param <T> inner type of s1
         * @param <I> inner type of s2
         * @return {@link Supplier} of {@link Boolean} type
         */
        public static <T extends Number, I extends Number> Supplier<Boolean> greaterThanEquals(Supplier<T> s1, Supplier<I> s2) {
            return createSupplier(
                s1,
                s2,
                String.format("greaterThanEquals(%s, %s)", s1.getIdentifier(), s2.getIdentifier()),
                (NumberSupplierLambda<T,I,Boolean>) (v1, v2) -> v1.doubleValue() >= v2.doubleValue()
            );
        }
    }
    
    public static class Caster implements Serializable {
        /** hidden constructor */
        private Caster(){
            throwUnsupportedOperationException();
        }
    
        /**
         * Casting operation from T type to {@link Long}
         *
         * @param supplier for T type value
         * @param <T> type of the supplied value
         * @return a {@link Supplier} of {@link Long} type
         */
        public static <T extends Number> Supplier<Long> castToLong(Supplier<T> supplier){
            String identifier = String.format("castToInteger(%s)", supplier.getIdentifier());
            return new NamedSupplier<>(
                identifier,
                values -> {
                    T extract = supplier.extract(values);
                    if(extract == null){
                        return null;
                    }
                    return extract.longValue();
                },
                () -> new HashSet<>(supplier.getChannelIdentifiers())
            );
        }
    
        /**
         * Casting operation from T type to {@link Double}
         *
         * @param supplier for T type value
         * @param <T> type of the supplied value
         * @return a {@link Supplier} of {@link Double} type
         */
        public static <T extends Number> Supplier<Double> castToDouble(Supplier<T> supplier){
            String identifier = String.format("castToDouble(%s)", supplier.getIdentifier());
            return new NamedSupplier<>(
                identifier,
                values -> {
                    T extract = supplier.extract(values);
                    if(extract == null){
                        return null;
                    }
                    return extract.doubleValue();
                },
                () -> new HashSet<>(supplier.getChannelIdentifiers())
            );
        }
    
        /**
         * Casting operation from T type to {@link String}
         *
         * @param supplier for T type value
         * @param <T> type of the supplied value
         * @return a {@link Supplier} of {@link String} type
         */
        public static <T extends Number> Supplier<String> castToString(Supplier<T> supplier){
            String identifier = String.format("castToDouble(%s)", supplier.getIdentifier());
            return new NamedSupplier<>(
                identifier,
                values -> {
                    T extract = supplier.extract(values);
                    if(extract == null){
                        return null;
                    }
                    return extract.toString();
                },
                () -> new HashSet<>(supplier.getChannelIdentifiers())
            );
        }
    }
    
    public static class Parser implements Serializable {
        /** hidden constructor */
        private Parser(){
            throwUnsupportedOperationException();
        }
    
    
        /**
         * Parsing operation from {@link String} to {@link Long}
         *
         * @param supplier for the {@link String} values to be parsed
         * @return a {@link Supplier} of {@link Long} type
         */
        public static Supplier<Long> parseLong(Supplier<String> supplier){
            String identifier = String.format("parseLong(%s)", supplier.getIdentifier());
            return new NamedSupplier<>(
                identifier,
                values -> Long.parseLong(supplier.extract(values)),
                () -> new HashSet<>(supplier.getChannelIdentifiers())
            );
        }
    
        /**
         * Parsing operation from {@link String} to {@link Double}
         *
         * @param supplier for the {@link String} values to be parsed
         * @return a {@link Supplier} of {@link Double} type
         */
        public static Supplier<Double> parseDouble(Supplier<String> supplier){
            String identifier = String.format("parseDouble(%s)", supplier.getIdentifier());
            return new NamedSupplier<>(
                identifier,
                values -> Double.parseDouble(supplier.extract(values)),
                () -> new HashSet<>(supplier.getChannelIdentifiers())
            );
        }
    }
    
    public static class Mathematics implements Serializable{
        private static final String ADD_NAME = "add(%s, %s)";
        private static final String SUBTRACT_NAME = "subtract(%s, %s)";
        private static final String MULTIPLY_NAME = "multiply(%s, %s)";
        private static final String DIVIDE_NAME = "divide(%s, %s)";
    
        /** hidden constructor */
        private Mathematics() {
            throwUnsupportedOperationException();
        }
    
        /**
         * Arithmetical operation: supplier + value
         *
         * @param value      first operand
         * @param supplier   second operand supplier
         * @param <T> type of value
         * @param <I> inner type of supplier
         * @return {@link Supplier} of Double type
         */
        public static <T extends Number, I extends Number> Supplier<Double> add(Supplier<T> supplier, I value){
            return createSupplier(
                value,
                supplier,
                String.format(ADD_NAME, value, supplier.getIdentifier()),
                AggregationUtils::add
            );
        }
    
        /**
         * Arithmetical operation: value + supplier
         *
         * @param value      first operand
         * @param supplier   second operand supplier
         * @param <T> type of value
         * @param <I> inner type of supplier
         * @return {@link Supplier} of Double type
         */
        public static <T extends Number, I extends Number> Supplier<Double> add(I value, Supplier<T> supplier){
            return add(supplier, value);
        }
    
        /**
         * Arithmetical operation: s1 + s2
         *
         * @param s1 first operand supplier
         * @param s2 second operand supplier
         * @param <T> inner type of s1
         * @param <I> inner type of s2
         * @return {@link Supplier} of {@link Boolean} type
         */
        public static <T extends Number, I extends Number> Supplier<Double> add(Supplier<T> s1, Supplier<I> s2){
            return createSupplier(
                s1,
                s2,
                String.format(ADD_NAME, s1.getIdentifier(), s2.getIdentifier()),
                (SupplierLambda<T, I, Double>) AggregationUtils::add
            );
        }
    
        /**
         * Arithmetical operation: supplier - value
         *
         * @param value      first operand
         * @param supplier   second operand supplier
         * @param <T> type of value
         * @param <I> inner type of supplier
         * @return {@link Supplier} of Double type
         */
        public static <T extends Number, I extends Number> Supplier<Double> subtract(Supplier<T> supplier, I value){
            return createSupplier(
                value,
                supplier,
                String.format(SUBTRACT_NAME, value, supplier.getIdentifier()),
                (v1, v2) -> AggregationUtils.subtract(v2, v1)
            );
        }
    
        /**
         * Arithmetical operation: value - supplier
         *
         * @param value      first operand
         * @param supplier   second operand supplier
         * @param <T> type of value
         * @param <I> inner type of supplier
         * @return {@link Supplier} of Double type
         */
        public static <T extends Number, I extends Number> Supplier<Double> subtract(I value, Supplier<T> supplier){
            return createSupplier(
                value,
                supplier,
                String.format(SUBTRACT_NAME, value, supplier.getIdentifier()),
                AggregationUtils::subtract
            );
        }
    
        /**
         * Arithmetical operation: s1 - s2
         *
         * @param s1 first operand supplier
         * @param s2 second operand supplier
         * @param <T> inner type of s1
         * @param <I> inner type of s2
         * @return {@link Supplier} of {@link Boolean} type
         */
        public static <T extends Number, I extends Number> Supplier<Double> subtract(Supplier<T> s1, Supplier<I> s2){
            return createSupplier(
                s1,
                s2,
                String.format(SUBTRACT_NAME, s1.getIdentifier(), s2.getIdentifier()),
                (SupplierLambda<T, I, Double>) AggregationUtils::subtract
            );
        }
    
        /**
         * Arithmetical operation: value * supplier
         *
         * @param value      first operand
         * @param supplier   second operand supplier
         * @param <T> type of value
         * @param <I> inner type of supplier
         * @return {@link Supplier} of Double type
         */
        public static <T extends Number, I extends Number> Supplier<Double> multiply(Supplier<T> supplier, I value){
            return createSupplier(
                value,
                supplier,
                String.format(MULTIPLY_NAME, value, supplier.getIdentifier()),
                AggregationUtils::multiply
            );
        }
    
        /**
         * Arithmetical operation: value * supplier
         *
         * @param value      first operand
         * @param supplier   second operand supplier
         * @param <T> type of value
         * @param <I> inner type of supplier
         * @return {@link Supplier} of Double type
         */
        public static <T extends Number, I extends Number> Supplier<Double> multiply(I value, Supplier<T> supplier){
            return multiply(supplier, value);
        }
    
        /**
         * Arithmetical operation: s1 * s2
         *
         * @param s1 first operand supplier
         * @param s2 second operand supplier
         * @param <T> inner type of s1
         * @param <I> inner type of s2
         * @return {@link Supplier} of {@link Boolean} type
         */
        public static <T extends Number, I extends Number> Supplier<Double> multiply(Supplier<T> s1, Supplier<I> s2){
            return createSupplier(
                s1,
                s2,
                String.format(MULTIPLY_NAME, s1.getIdentifier(), s2.getIdentifier()),
                (SupplierLambda<T, I, Double>) AggregationUtils::multiply
            );
        }
    
        /**
         * Arithmetical operation: supplier / value
         *
         * @param value      first operand
         * @param supplier   second operand supplier
         * @param <T> type of value
         * @param <I> inner type of supplier
         * @return {@link Supplier} of Double type
         */
        public static <T extends Number, I extends Number> Supplier<Double> divide(Supplier<T> supplier, I value){
            return createSupplier(
                value,
                supplier,
                String.format(DIVIDE_NAME, value, supplier.getIdentifier()),
                (v1, v2) -> AggregationUtils.divide(v2, v1)
            );
        }
    
        /**
         * Arithmetical operation: value / supplier
         *
         * @param value      first operand
         * @param supplier   second operand supplier
         * @param <T> type of value
         * @param <I> inner type of supplier
         * @return {@link Supplier} of Double type
         */
        public static <T extends Number, I extends Number> Supplier<Double> divide(I value, Supplier<T> supplier){
            return createSupplier(
                value,
                supplier,
                String.format(DIVIDE_NAME, value, supplier.getIdentifier()),
                AggregationUtils::divide
            );
        }
    
        /**
         * Arithmetical operation: s1 / s2
         *
         * @param s1 first operand supplier
         * @param s2 second operand supplier
         * @param <T> inner type of s1
         * @param <I> inner type of s2
         * @return {@link Supplier} of {@link Boolean} type
         */
        public static <T extends Number, I extends Number> Supplier<Double> divide(Supplier<T> s1, Supplier<I> s2){
            return createSupplier(
                s1,
                s2,
                String.format(DIVIDE_NAME, s1.getIdentifier(), s2.getIdentifier()),
                (NumberSupplierLambda<T, I, Double>) AggregationUtils::divide
            );
        }
    }
    
    /**
     * creates a Supplier that operates on two Suppliers
     *
     * @param value      first operand
     * @param supplier   second operand supplier
     * @param identifier of the {@link Supplier} to be created
     * @param lambda     the inner operation to be performed
     * @param <T> type of value
     * @param <I> inner type of supplier
     * @param <R> inner type of the resulting {@link Supplier}
     * @return {@link Supplier} of R type
     */
    private static <T extends Serializable, I extends Serializable, R extends Serializable> Supplier<R> createSupplier(
        T value,
        Supplier<I> supplier,
        String identifier,
        SupplierLambda<T,I,R> lambda
    ) {
        return new NamedSupplier<>(
            identifier,
            values -> {
                // extract value
                I value2 = supplier.extract(values);
                if(value2 == null){
                    return null;
                }
                return lambda.apply(value, value2);
            },
            () -> new HashSet<>(supplier.getChannelIdentifiers())
        );
    }
    
    /**
     * creates a Supplier that operates on two Suppliers
     *
     * @param s1 first operand supplier
     * @param s2 second operand supplier
     * @param identifier of the {@link Supplier} to be created
     * @param lambda the inner operation to be performed
     * @param <T> inner type of s1
     * @param <I> inner type of s2
     * @param <R> inner type of the resulting {@link Supplier}
     * @return {@link Supplier} of R type
     */
    private static <T extends Serializable, I extends Serializable, R extends Serializable> Supplier<R> createSupplier(
        Supplier<T> s1,
        Supplier<I> s2,
        String identifier,
        SupplierLambda<T,I,R> lambda
    ) {
        return new NamedSupplier<>(
            identifier,
            values -> {
                // extract values
                T value = s1.extract(values);
                I value2 = s2.extract(values);
                if(value == null || value2 == null){
                    return null;
                }
                return lambda.apply(value, value2);
            },
            () -> combine(s1, s2)
        );
    }
    
    /** @inheritDoc */
    @FunctionalInterface
    interface NumberSupplierLambda<
        T extends Number,
        I extends Number,
        R extends Serializable> extends SupplierLambda<T,I,R> {
    }
    
    /** @inheritDoc */
    @FunctionalInterface
    interface ComparableSupplierLambda<
        T extends Comparable & Serializable,
        I extends Comparable & Serializable,
        R extends Serializable> extends SupplierLambda<T,I,R> { }
    
    /**
     * Lambda definition interface for definition of operations inside a {@link Supplier} that is created by the
     * {@link #createSupplier(Serializable, Supplier, String, SupplierLambda)} and the
     * {@link #createSupplier(Supplier, Supplier, String, SupplierLambda)} methods.
     *
     * @param <T> type of the first operand
     * @param <I> type of the second operand
     * @param <R> type of the result
     */
    @FunctionalInterface
    interface SupplierLambda<T extends Serializable, I extends Serializable, R extends Serializable> {
        R apply(T v1, I v2);
    }
    
    /**
     * Utility class for channel identifier extraction
     */
    static final class IdentifierCombiner implements Serializable {
        /** hidden constructor **/
        private IdentifierCombiner() {
            throwUnsupportedOperationException();
        }
    
        /**
         * Combines the identifiers of two inner {@link Supplier}s
         *
         * @param s1 first {@link Supplier}
         * @param s2 second {@link Supplier}
         * @param <T> type of the {@link Supplier}s
         * @return ArrayList of String with all identifiers
         */
        @SuppressWarnings("squid:S1319") // return type is not an interface for being serializable
        public static <T extends Serializable, I extends Serializable> HashSet<String> combine(Supplier<T> s1, Supplier<I> s2){
            HashSet<String> results = new HashSet<>(s1.getChannelIdentifiers());
            results.addAll(s2.getChannelIdentifiers());
            return results;
        }
    }
}
