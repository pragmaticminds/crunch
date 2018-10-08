package org.pragmaticminds.crunch.execution;

import org.junit.Test;
import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.UntypedValues;

import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * @author julian
 * Created by julian on 15.08.18
 */
public class MRecordSourcesTest {
    
    @Test
    public void of_Array() {
        MRecordSource source = MRecordSources.of(new UntypedValues(), new UntypedValues());

        assertTwoElements(source);
    }

    @Test
    public void of_List() {
        MRecordSource source = MRecordSources.of(Arrays.asList(new UntypedValues(), new UntypedValues()));

        assertTwoElements(source);
    }

    @Test
    public void of_Iterator() {
        Iterator<MRecord> iterator = Arrays.<MRecord>asList(new UntypedValues(), new UntypedValues()).iterator();
        MRecordSource source = MRecordSources.of(iterator);

        assertTwoElements(source);
        assertEquals(MRecordSource.Kind.UNKNOWN, source.getKind());
    }

    @Test
    public void of_IteratorWithKind() {
        Iterator<MRecord> iterator = Arrays.<MRecord>asList(new UntypedValues(), new UntypedValues()).iterator();
        MRecordSource source = MRecordSources.of(MRecordSource.Kind.FINITE, iterator);

        assertTwoElements(source);
        assertEquals(MRecordSource.Kind.FINITE, source.getKind());
    }
    
    /**
     * Asserts that the source has exactly two elements.
     *
     * @param source
     */
    private static void assertTwoElements(MRecordSource source) {
        assertTrue(source.hasRemaining());
        source.get();
        assertTrue(source.hasRemaining());
        source.get();
        assertFalse(source.hasRemaining());
    }
}