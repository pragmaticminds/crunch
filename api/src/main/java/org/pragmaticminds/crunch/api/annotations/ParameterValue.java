package org.pragmaticminds.crunch.api.annotations;

import org.pragmaticminds.crunch.api.records.DataType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This Annotation is used like {@link ChannelValue} but indicates the Value of an Scalar FunctionParameter.
 * This FunctionParameter is injected one (before the Setup Method is called).
 * <p>
 * Created by julian on 13.02.17.
 *
 * @deprecated Part of the old API
 */
@Deprecated
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ParameterValue {

    String name();

    DataType dataType();
}
