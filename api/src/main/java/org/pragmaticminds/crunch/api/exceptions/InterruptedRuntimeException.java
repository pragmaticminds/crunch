package org.pragmaticminds.crunch.api.exceptions;

/**
 * Wrapper for the {@link InterruptedException} as a {@link RuntimeException}.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 03.10.2018
 */
public class InterruptedRuntimeException extends RuntimeException {
    
    /**
     * Constructor wraps an {@link InterruptedException} into a {@link RuntimeException}.
     *
     * @param cause the original {@link RuntimeException}.
     */
    public InterruptedRuntimeException(InterruptedException cause) {
        super(cause);
    }
}
