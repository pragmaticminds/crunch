package org.pragmaticminds.crunch.api.annotations;

import org.pragmaticminds.crunch.api.EvalFunction;
import org.pragmaticminds.crunch.api.records.DataType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The type setting of a result or event fired by a {@link EvalFunction}
 * <p>
 * Created by Erwin Wagasow on 23.05.2017.
 *
 * @deprecated Part of the old API
 */
@Deprecated
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ResultType {
    String name();

    DataType dataType();
}
