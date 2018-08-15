package org.pragmaticminds.crunch.execution;

import org.pragmaticminds.crunch.api.records.MRecord;

import java.io.Serializable;

/**
 * Source for {@link MRecordSource} can be finite or infinite.
 *
 * @author julian
 * Created by julian on 15.08.18
 */
interface MRecordSource extends Serializable {

    /**
     * Request the next record
     *
     * @return record
     */
    MRecord get();

    /**
     * Check wheter more records are available for fetch
     *
     * @return true if records can be fetched using {@link #get()}
     */
    boolean hasRemaining();

    /**
     * Is called before first call to {@link #hasRemaining()} or {@link #get()}.
     */
    void init();

    /**
     * Is called after processing has ended (either by cancelling or by exhausting the source).
     */
    void close();

    /**
     * Returns the Kind of the record source.
     *
     * @return Kind of the source.
     */
    Kind getKind();

    /**
     * Specifies the Kind of this MRecordSource.
     */
    enum Kind {
        FINITE,
        INFINITE,
        UNKNOWN
    }
}
