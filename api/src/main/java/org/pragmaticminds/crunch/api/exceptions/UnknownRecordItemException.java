package org.pragmaticminds.crunch.api.exceptions;

import org.pragmaticminds.crunch.api.records.MRecord;

/**
 * Is thrown by getters of {@link MRecord} when they getValue a reqiuest for an unknown channel.
 */
public class UnknownRecordItemException extends RuntimeException {

    public UnknownRecordItemException(String message) {
        super(message);
    }
}
