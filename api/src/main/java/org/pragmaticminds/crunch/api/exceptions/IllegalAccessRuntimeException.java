package org.pragmaticminds.crunch.api.exceptions;

/**
 * Wrapper around the {@link IllegalAccessException} as a {@link RuntimeException}
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 23.08.2018
 */
public class IllegalAccessRuntimeException extends RuntimeException {
    /**
     * Wrapper constructor
     * @param e exception to wrap
     */
    public IllegalAccessRuntimeException(IllegalAccessException e) {
        super(e);
    }
}
