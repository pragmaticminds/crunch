package org.pragmaticminds.crunch.api;

import java.io.Serializable;

/**
 * Factory Pattern to create EvalFunctions.
 */
@FunctionalInterface
public interface EvalFunctionFactory extends Serializable {

    EvalFunction create() throws IllegalAccessException;
}
