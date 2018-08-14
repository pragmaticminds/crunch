package org.pragmaticminds.crunch.api.records;

/**
 * Is thrown by type safe getters of {@link MRecord} when the requested channel exists and cannot be casted
 * to the requested type.
 */
public class RecordItemConversionException extends RuntimeException {

    public RecordItemConversionException(String message) {
        super(message);
    }

    public RecordItemConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}
