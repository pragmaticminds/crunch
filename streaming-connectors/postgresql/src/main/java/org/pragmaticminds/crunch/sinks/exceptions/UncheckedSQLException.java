package org.pragmaticminds.crunch.sinks.exceptions;

import java.sql.SQLException;

/**
 * Wrapper for the {@link SQLException} into a {@link RuntimeException}
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 13.09.2018
 */
public class UncheckedSQLException extends RuntimeException {
    /**
     * Main constructor, which is passing trough values to parent.
     * @param message of the error situation
     * @param ex original exception
     */
    public UncheckedSQLException(String message, SQLException ex) {
        super(message, ex);
    }
}
