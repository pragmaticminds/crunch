package org.pragmaticminds.crunch.api.state;

/**
 * This exception is thrown when the timeout in the ChainedEvalFunction is reached
 *
 * @author kerstin
 * Created by kerstin on 05.09.18.
 */
public class OverallTimeoutException extends Exception {
    public OverallTimeoutException(String message) {
        super(message);
    }
}
