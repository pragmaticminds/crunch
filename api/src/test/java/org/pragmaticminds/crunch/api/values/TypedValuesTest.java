package org.pragmaticminds.crunch.api.values;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.mql.DataType;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link TypedValues}
 *
 * @author julian
 * Created by julian on 03.11.17
 */
public class TypedValuesTest {

    private TypedValues values;

    @Before
    public void setUp() {
        values = createValues();
    }

    private TypedValues createValues() {
        return TypedValues.builder()
                .source("no_source")
                .timestamp(100)
                .values(new HashMap<>())
                .build();
    }

    @Test
    public void getTypedValues() {
        fillTypedValues(values);

        assertEquals(3.141, values.getDouble("double"), 1e-16);
        assertEquals(100, values.getLong("long"));
        assertEquals("hallo", values.getString("String"));
        assertEquals(false, values.getBoolean("boolean"));
    }

    private void fillTypedValues(TypedValues values) {
        values.getValues().put("double", Value.of(3.141));
        values.getValues().put("long", Value.of(100L));
        values.getValues().put("String", Value.of("hallo"));
        values.getValues().put("boolean", Value.of(false));
    }

    @Test(expected = IllegalArgumentException.class)
    public void merge_olderState_throwsException() {
        TypedValues values1 = TypedValues.builder()
                .source("no_source")
                .timestamp(90)
                .values(new HashMap<>())
                .build();

        values.merge(values1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void merge_differentSource_throwsException() {
        TypedValues values1 = TypedValues.builder()
                .source("different_source")
                .timestamp(101)
                .values(new HashMap<>())
                .build();

        values.merge(values1);
    }

    @Test
    public void merge_rightPreconditions_works() {
        TypedValues values = createValues();

        TypedValues values1 = TypedValues.builder()
                .source("no_source")
                .timestamp(101)
                .values(Collections.singletonMap("additional", Value.of(111)))
                .build();

        TypedValues merge = values.merge(values1);

        assertEquals(101, merge.getTimestamp());
        assertEquals(111, merge.getLong("additional"));
    }

    @Test
    public void merge_valueExists_overwritesOldOne() {
        TypedValues values = createValues();

        TypedValues values1 = TypedValues.builder()
                .source("no_source")
                .timestamp(101)
                .values(Collections.singletonMap("long", Value.of(111)))
                .build();

        TypedValues merge = values.merge(values1);

        assertEquals(101, merge.getTimestamp());
        assertEquals(111, merge.getLong("long"));
    }

    @Test
    public void get_shouldReturnRightValue() {
        TypedValues values = createValues();
        fillTypedValues(values);

        assertEquals(3.141, values.get("double", DataType.DOUBLE));
        assertEquals(100L, values.get("long", DataType.LONG));
        assertEquals("hallo", values.get("String", DataType.STRING));
        assertEquals(false, values.get("boolean", DataType.BOOL));
    }
}