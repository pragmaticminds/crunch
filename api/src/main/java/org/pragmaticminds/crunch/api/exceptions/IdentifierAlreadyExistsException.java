package org.pragmaticminds.crunch.api.exceptions;

/**
 * This exception is thrown when an identifier already is in use.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 03.08.2018
 */
public class IdentifierAlreadyExistsException extends RuntimeException {
    /**
     * @param s is the description of the exception conditions
     */
    public IdentifierAlreadyExistsException(String s) {
        super(s);
    }
}
