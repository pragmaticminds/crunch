package org.pragmaticminds.crunch.api.trigger.comparator;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.BooleanOperators.*;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.*;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.StringOperators.*;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 13.08.2018
 */
public class SuppliersTest {
    private TypedValues values;
    
    @Before
    public void setUp() throws Exception {
        Map<String, Value> valueMap = new HashMap<>();
        valueMap.put("boolean", Value.of(true));
        valueMap.put("booleanFalse", Value.of(false));
        valueMap.put("string", Value.of("string"));
        valueMap.put("long", Value.of(123L));
        valueMap.put("date", Value.of(Date.from(Instant.now())));
        valueMap.put("double", Value.of(0.1D));
        values = TypedValues.builder().timestamp(System.currentTimeMillis()).source("test").values(valueMap).build();
    }
    
    @Test
    public void booleanChannelTest(){
        Supplier<Boolean> supplier = booleanChannel("boolean");
        Boolean extract = supplier.extract(values);
        Assert.assertEquals(true, extract);
    }
    
    @Test
    public void doubleChannelTest(){
        Supplier<Double> supplier = doubleChannel("double");
        Double extract = supplier.extract(values);
        Assert.assertEquals(0.1f, extract, 0.001);
    }
    
    @Test
    public void longChannelTest(){
        Supplier<Long> supplier = longChannel("long");
        Long extract = supplier.extract(values);
        Assert.assertEquals(123L, (long)extract);
    }
    
    @Test
    public void dateChannelTest(){
        Supplier<Date> supplier = dateChannel("date");
        Date extract = supplier.extract(values);
        Assert.assertNotNull(extract);
    }
    
    @Test
    public void stringChannelTest(){
        Supplier<String> supplier = stringChannel("string");
        String extract = supplier.extract(values);
        Assert.assertEquals("string", extract);
    }
    
    @Test
    public void andTest(){
        Supplier<Boolean> supplier = and(booleanChannel("boolean"), booleanChannel("boolean"));
        Boolean extract = supplier.extract(values);
        Assert.assertTrue(extract);
    }
    
    @Test
    public void andTest2(){
        Supplier<Boolean> supplier = and(booleanChannel("boolean"), booleanChannel("booleanFalse"));
        Boolean extract = supplier.extract(values);
        Assert.assertFalse(extract);
    }
    
    @Test
    public void orTest(){
        Supplier<Boolean> supplier = or(booleanChannel("boolean"), booleanChannel("booleanFalse"));
        Boolean extract = supplier.extract(values);
        Assert.assertTrue(extract);
    }
    
    @Test
    public void orTest2(){
        Supplier<Boolean> supplier = or(booleanChannel("booleanFalse"), booleanChannel("booleanFalse"));
        Boolean extract = supplier.extract(values);
        Assert.assertFalse(extract);
    }
    
    @Test
    public void notTest(){
        Supplier<Boolean> supplier = not(booleanChannel("booleanFalse"));
        Boolean extract = supplier.extract(values);
        Assert.assertTrue(extract);
    }
    
    @Test
    public void equalsTest(){
        Supplier<Boolean> supplier = equal("string", stringChannel("string"));
        Boolean extract = supplier.extract(values);
        Assert.assertTrue(extract);
    }
    
    @Test
    public void equalsTest2(){
        Supplier<Boolean> supplier = equal(stringChannel("string"), stringChannel("string"));
        Boolean extract = supplier.extract(values);
        Assert.assertTrue(extract);
    }
    
    @Test
    public void matchTest(){
        Supplier<Boolean> supplier = match("s.*g", stringChannel("string"));
        Boolean extract = supplier.extract(values);
        Assert.assertTrue(extract);
    }
    
    @Test
    public void containsTest(){
        Supplier<Boolean> supplier = contains("str", stringChannel("string"));
        Boolean extract = supplier.extract(values);
        Assert.assertTrue(extract);
    }
    
    @Test
    public void lengthTest(){
        Supplier<Long> supplier = length(stringChannel("string"));
        long extract = supplier.extract(values);
        Assert.assertEquals(6L, extract);
    }
    
    @Test
    public void comparatorEquals() {
        Supplier<Boolean> supplier = Suppliers.Comparators.equals(0.1D, doubleChannel("double"));
        boolean extract = supplier.extract(values);
        Assert.assertTrue(extract);
    }
    
    @Test
    public void comparatorEquals2() {
        Supplier<Boolean> supplier = Suppliers.Comparators.equals(doubleChannel("double"), doubleChannel("double"));
        boolean extract = supplier.extract(values);
        Assert.assertTrue(extract);
    }
    
    @Test
    public void comparatorCompare() {
        Supplier<Long> supplier = Suppliers.Comparators.compare(0.1D, doubleChannel("double"));
        Long extract = supplier.extract(values);
        Assert.assertEquals(0L, (long)extract);
    }
    
    @Test
    public void comparatorCompare2() {
        Supplier<Long> supplier = Suppliers.Comparators.compare(doubleChannel("double"), doubleChannel("double"));
        Long extract = supplier.extract(values);
        Assert.assertEquals(0L, (long)extract);
    }
}