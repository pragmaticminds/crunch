package org.pragmaticminds.crunch.api.windowed;

import org.pragmaticminds.crunch.api.records.MRecord;

import java.io.Serializable;

/**
 * This Interface checks if a record window is opened for accumulation of records or if it is closed to drop all
 * accumulated records and the current record.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 16.08.2018
 */
@FunctionalInterface
public interface RecordWindow extends Serializable {
    /**
     * Checks if a processing window is open or closed.
     * In case of open -> all records will be accumulated in the next processing instance of the record.
     * In case of closed -> all records and the current are dropped in the next processing instance of the record.
     * @param record is checked if a window is open or closed
     * @return true if window is open, false otherwise
     */
    boolean inWindow(MRecord record);
}
