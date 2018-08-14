package org.pragmaticminds.crunch.api.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Collection wrapper for ResultType of EventType object annotations
 * <p>
 * Created by k3r5t1n on 4/7/17.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ResultTypes {
    ResultType[] resultTypes();
}
