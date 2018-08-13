package org.pragmaticminds.crunch.api.values.dates;

/**
 * This Visitor implements the superior and beloved Visitor Pattern.
 * It is usefull when someone wants to transform {@link Value}'s into something else and has to react to each
 * subclass of {@link Value} differently.
 * <p>
 * Has to be extended for all new subclasses of {@link Value}.
 *
 * @author julian
 * Created by julian on 13.11.17
 */
public interface ValueVisitor<T> {

    T visit(BooleanValue value);

    T visit(DateValue value);

    T visit(DoubleValue value);

    T visit(LongValue value);

    T visit(StringValue value);

}
