package org.pragmaticminds.crunch.api.values.dates;

import org.junit.Test;

import java.security.InvalidParameterException;
import java.time.Instant;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 *
 * @author kerstin
 * Created by kerstin on 06.11.17.
 */
public class ValueTest {

    @Test
    public void ofObject_boolean() {
        Object o = Boolean.FALSE;
        Value of = Value.of(o);
        assertFalse(of.getAsBoolean());
    }

    @Test
    public void ofObject_string() {
        Object o = "test";
        Value of = Value.of(o);
        assertEquals("test", of.getAsString());
    }

    @Test
    public void ofObject_date() {
        Date date = Date.from(Instant.now());
        Value of = Value.of((Object) date);
        assertEquals(date, of.getAsDate());
    }

    @Test
    public void ofObject_double() {
        Object o = 3.14;
        Value of = Value.of(o);
        assertEquals(3.14, of.getAsDouble(), 0.001);
    }

    @Test
    public void ofObject_long() {
        Object o = 42L;
        Value of = Value.of(o);
        assertEquals(new Long(42L), of.getAsLong());
    }
    
    @Test(expected = InvalidParameterException.class)
    public void ofObject_fails() {
        Object o = Instant.now();
        Value.of(o);
    }
    
    @Test
    public void ofNull() {
        Object o = null;
        Value.of(o);
    }

}