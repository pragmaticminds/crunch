package org.pragmaticminds.crunch.api.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This Annotation can be used on Fields of long in an {@code EvaluationFunction}.
 * It indicates that the field will getValue the time value injected.
 *
 * @author kerstin
 * Created by kerstin on 15.11.17.
 *
 * @deprecated Part of the old API
 */
@Deprecated
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface TimeValue {
}
