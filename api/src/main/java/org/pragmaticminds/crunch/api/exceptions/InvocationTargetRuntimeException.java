package org.pragmaticminds.crunch.api.exceptions;

import java.lang.reflect.InvocationTargetException;

/**
 * Wrapper around {@link InvocationTargetException} as a {@link RuntimeException}
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 23.08.2018
 */
public class InvocationTargetRuntimeException extends RuntimeException {
    public InvocationTargetRuntimeException(InvocationTargetException e) {
        super(e);
    }
}
