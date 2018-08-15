package org.pragmaticminds.crunch.api.pipe;

import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.UntypedValues;

import java.io.Serializable;

/**
 * This predicate implements the filtering of incoming {@link UntypedValues} to separate those which are to
 * be processed.
 *
 * @author Erwin Wagasow
 * craeted by Erwin Wagasow on 03.08.2018
 */
@FunctionalInterface
public interface SubStreamPredicate extends Serializable {
    /**
     * Validates incoming {@link MRecord} if to be processed in that {@link SubStream}
     * @param values to be validated
     * @return true if the {@link MRecord} is to be processed
     */
    Boolean validate(MRecord values);
}
