package org.pragmaticminds.crunch.api.exceptions;

/**
 * This exception is thrown when a cloning process fails
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 03.08.2018
 */
public class CloneFailedException extends RuntimeException {
    /**
     * packs the original exception in this exception
     *
     * @param ex the original exception
     */
    public CloneFailedException(Exception ex) {
        super(ex);
    }
}
