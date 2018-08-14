package org.pragmaticminds.crunch.api.annotations;

import org.pragmaticminds.crunch.api.records.DataType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This Annotation is necessary for classes to be used as EvalFunctions.
 * Futhermore all classes have to implement the EvalFunctions interface.
 * <p>
 * Created by julian on 13.02.17.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface EvaluationFunction {
    String evaluationName();

    DataType dataType();

    String description();
}



