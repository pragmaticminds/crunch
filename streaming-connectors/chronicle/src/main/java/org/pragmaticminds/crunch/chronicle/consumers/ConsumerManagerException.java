package org.pragmaticminds.crunch.chronicle.consumers;

/**
 * Exception when problems with the Database occur.
 */
class ConsumerManagerException extends RuntimeException {

    public ConsumerManagerException(String s, Exception e) {
        super(s, e);
    }

    public ConsumerManagerException(String message) {
        super(message);
    }
}
