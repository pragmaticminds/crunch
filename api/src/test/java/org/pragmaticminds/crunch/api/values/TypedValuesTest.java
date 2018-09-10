package org.pragmaticminds.crunch.api.values;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.*;

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
        assertEquals(100L, (long)values.getLong("long"));
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
        assertEquals(111, (long)merge.getLong("additional"));
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
        assertEquals(111, (long)merge.getLong("long"));
    }
    
    @Test
    public void toStringTest(){
        TypedValues values = TypedValues.builder()
            .source("test")
            .timestamp(123L)
            .values(Collections.singletonMap("key", Value.of("test")))
            .build();
        String string = values.toString();
        assertNotNull(string);
        assertFalse(string.isEmpty());
        assertEquals(
            "TypedValues(source=test, timestamp=123, values={key=StringValue(value=test)})",
            string
        );
    }
    
    @Test
    public void toStringWithNullsTest(){
        // set no values in the TypedValues
        TypedValues values = TypedValues.builder().build();
        String string = values.toString();
        assertNotNull(string);
        assertFalse(string.isEmpty());
        assertEquals("TypedValues(source=null, timestamp=0, values=null)", string);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void get_shouldReturnRightValue() {
        TypedValues values = createValues();
        fillTypedValues(values);

        Object aDouble = values.get("double");
    }
}