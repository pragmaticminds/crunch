package org.pragmaticminds.crunch.api.annotations;

import org.pragmaticminds.crunch.api.records.DataType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This Annotation can be used on Fields of specific Type in an {@code EvaluationFunction}.
 * It indicates that the field will getValue the channel value injected.
 * Created by julian on 13.02.17.
 *
 * @deprecated Part of the old API
 */
@Deprecated
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ChannelValue {

    /**
     * Name of the ChannelParameter that should be injected.
     *
     * @return the name of the channel as String
     */
    String name();

    /**
     * Type of the Channel.
     *
     * @return the data type of the channel as DataType
     */
    DataType dataType();
}
