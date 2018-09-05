package org.pragmaticminds.crunch.api.state;

/**
 * This exception is thrown when the timeout in a state of the ChainedEvalFunction is reached
 *
 * @author kerstin
 * Created by kerstin on 05.09.18.
 */
public class StepTimeoutException extends Exception {
    public StepTimeoutException(String message) {
        super(message);
    }
}
