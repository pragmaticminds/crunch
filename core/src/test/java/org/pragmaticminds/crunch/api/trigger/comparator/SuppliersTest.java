/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.pragmaticminds.crunch.api.trigger.comparator;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;
import org.pragmaticminds.crunch.api.values.TypedValues;
import org.pragmaticminds.crunch.api.values.dates.Value;

import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.BooleanOperators.and;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.BooleanOperators.not;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.BooleanOperators.or;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.booleanChannel;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.channel;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.dateChannel;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.doubleChannel;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.longChannel;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.stringChannel;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.StringOperators.contains;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.StringOperators.equal;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.StringOperators.length;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.StringOperators.match;

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
        valueMap.put("double2", Value.of(-0.1D));
        valueMap.put("stringLong", Value.of("123"));
        valueMap.put("stringDouble", Value.of("0.1"));
        values = TypedValues.builder().timestamp(System.currentTimeMillis()).source("test").values(valueMap).build();
    }

    @Test
    public void booleanChannelTest(){
        // value test
        Supplier<Boolean> supplier = booleanChannel("boolean");
        Boolean extract = supplier.extract(values);
        assertEquals(true, extract);

        assertTrue(supplier.getChannelIdentifiers().contains("boolean"));

        // null test
        Supplier<Boolean> nullSupplier = booleanChannel("null");
        extract = nullSupplier.extract(values);
        Assert.assertNull(extract);

        // serializable test
        Supplier<Boolean> clone = ClonerUtil.clone(supplier);
        Boolean clonedExtract = clone.extract(values);
        assertEquals(true, clonedExtract);
    }

    @Test
    public void doubleChannelTest(){
        // value test
        Supplier<Double> supplier = doubleChannel("double");
        Double extract = supplier.extract(values);
        assertEquals(0.1f, extract, 0.0001);

        assertTrue(supplier.getChannelIdentifiers().contains("double"));

        // null test
        Supplier<Double> nullSupplier = doubleChannel("null");
        extract = nullSupplier.extract(values);
        Assert.assertNull(extract);

        // serializable test
        Supplier<Double> clone = ClonerUtil.clone(supplier);
        Double clonedExtract = clone.extract(values);
        assertEquals(0.1D, clonedExtract, 0.0001);
    }

    @Test
    public void longChannelTest(){
        // value test
        Supplier<Long> supplier = longChannel("long");
        Long extract = supplier.extract(values);
        assertEquals(123L, (long)extract);

        assertTrue(supplier.getChannelIdentifiers().contains("long"));

        // null test
        Supplier<Long> nullSupplier = longChannel("null");
        extract = nullSupplier.extract(values);
        Assert.assertNull(extract);

        // serializable test
        Supplier<Long> clone = ClonerUtil.clone(supplier);
        Long clonedExtract = clone.extract(values);
        assertEquals(123L, (long)clonedExtract);
    }

    @Test
    public void dateChannelTest(){
        // value test
        Supplier<Date> supplier = dateChannel("date");
        Date extract = supplier.extract(values);
        Assert.assertNotNull(extract);

        assertTrue(supplier.getChannelIdentifiers().contains("date"));

        // null test
        Supplier<Date> nullSupplier = dateChannel("null");
        extract = nullSupplier.extract(values);
        Assert.assertNull(extract);

        // serializable test
        Supplier<Date> clone = ClonerUtil.clone(supplier);
        Date clonedExtract = clone.extract(values);
        assertNotNull(clonedExtract);
    }

    @Test
    public void stringChannelTest(){
        // value test
        Supplier<String> supplier = stringChannel("string");
        String extract = supplier.extract(values);
        assertEquals("string", extract);

        assertTrue(supplier.getChannelIdentifiers().contains("string"));

        // null test
        Supplier<String> nullSupplier = stringChannel("null");
        extract = nullSupplier.extract(values);
        Assert.assertNull(extract);

        // serializable test
        Supplier<String> clone = ClonerUtil.clone(supplier);
        String clonedExtract = clone.extract(values);
        assertEquals("string", clonedExtract);
    }

    @Test
    public void channelsTest() {
        Supplier<Value> supplier = channel("string");
        String extract = supplier.extract(values).getAsString();
        assertEquals("string", extract);

        assertTrue(supplier.getChannelIdentifiers().contains("string"));

        Supplier<Value> supplier1 = channel("long");
        Long extract1 = supplier1.extract(values).getAsLong();
        assertEquals(123L, (long) extract1);

        // serializable test
        Supplier<Value> clone = ClonerUtil.clone(supplier);
        String clonedExtract = clone.extract(values).getAsString();
        assertEquals("string", clonedExtract);
    }

    @Test
    public void andTest(){
        // value test
        Supplier<Boolean> supplier = and(booleanChannel("boolean"), booleanChannel("boolean"));
        Boolean extract = supplier.extract(values);
        assertTrue(extract);

        assertTrue(supplier.getChannelIdentifiers().contains("boolean"));

        // null test
        Supplier<Boolean> nullSupplier = and(booleanChannel("null"), booleanChannel("boolean"));
        extract = nullSupplier.extract(values);
        Assert.assertNull(extract);

        // serializable test
        Supplier<Boolean> clone = ClonerUtil.clone(supplier);
        Boolean clonedExtract = clone.extract(values);
        assertEquals(true, clonedExtract);
    }

    @Test
    public void andTest2(){
        // value test
        Supplier<Boolean> supplier = and(booleanChannel("boolean"), booleanChannel("booleanFalse"));
        Boolean extract = supplier.extract(values);
        Assert.assertFalse(extract);

        assertTrue(supplier.getChannelIdentifiers().contains("boolean"));
        assertTrue(supplier.getChannelIdentifiers().contains("booleanFalse"));
    }

    @Test
    public void orTest(){
        // value test
        Supplier<Boolean> supplier = or(booleanChannel("boolean"), booleanChannel("booleanFalse"));
        Boolean extract = supplier.extract(values);
        assertTrue(extract);

        assertTrue(supplier.getChannelIdentifiers().contains("boolean"));
        assertTrue(supplier.getChannelIdentifiers().contains("booleanFalse"));

        // null test
        Supplier<Boolean> nullSupplier = or(booleanChannel("null"), booleanChannel("boolean"));
        extract = nullSupplier.extract(values);
        Assert.assertNull(extract);

        // serializable test
        Supplier<Boolean> clone = ClonerUtil.clone(supplier);
        Boolean clonedExtract = clone.extract(values);
        assertEquals(true, clonedExtract);
    }

    @Test
    public void orTest2(){
        // value test
        Supplier<Boolean> supplier = or(booleanChannel("booleanFalse"), booleanChannel("booleanFalse"));
        Boolean extract = supplier.extract(values);
        Assert.assertFalse(extract);

        assertTrue(supplier.getChannelIdentifiers().contains("booleanFalse"));
    }

    @Test
    public void notTest(){
        // value test
        Supplier<Boolean> supplier = not(booleanChannel("booleanFalse"));
        Boolean extract = supplier.extract(values);
        assertTrue(extract);

        assertTrue(supplier.getChannelIdentifiers().contains("booleanFalse"));

        // null test
        Supplier<Boolean> nullSupplier = not(booleanChannel("null"));
        extract = nullSupplier.extract(values);
        Assert.assertNull(extract);

        // serializable test
        Supplier<Boolean> clone = ClonerUtil.clone(supplier);
        Boolean clonedExtract = clone.extract(values);
        assertEquals(true, clonedExtract);
    }

    @Test
    public void equalsTest(){
        // value test
        Supplier<Boolean> supplier = equal("string", stringChannel("string"));
        Boolean extract = supplier.extract(values);
        assertTrue(extract);

        assertTrue(supplier.getChannelIdentifiers().contains("string"));

        // null test
        Supplier<Boolean> nullSupplier = equal("string", stringChannel("null"));
        extract = nullSupplier.extract(values);
        Assert.assertNull(extract);

        assertTrue(nullSupplier.getChannelIdentifiers().contains("null"));

        // serializable test
        Supplier<Boolean> clone = ClonerUtil.clone(supplier);
        Boolean clonedExtract = clone.extract(values);
        assertEquals(true, clonedExtract);
    }

    @Test
    public void equalsTest2(){
        // value test
        Supplier<Boolean> supplier = equal(stringChannel("string"), stringChannel("string"));
        Boolean extract = supplier.extract(values);
        assertTrue(extract);

        assertTrue(supplier.getChannelIdentifiers().contains("string"));

        // null test
        Supplier<Boolean> nullSupplier = equal(stringChannel("null"), stringChannel("string"));
        extract = nullSupplier.extract(values);
        Assert.assertNull(extract);

        assertTrue(nullSupplier.getChannelIdentifiers().contains("string"));
        assertTrue(nullSupplier.getChannelIdentifiers().contains("null"));
    }

    @Test
    public void matchTest(){
        // value test
        Supplier<Boolean> supplier = match("s.*g", stringChannel("string"));
        Boolean extract = supplier.extract(values);
        assertTrue(extract);

        assertTrue(supplier.getChannelIdentifiers().contains("string"));

        // null test
        Supplier<Boolean> nullSupplier = match("s.*g", stringChannel("null"));
        extract = nullSupplier.extract(values);
        Assert.assertNull(extract);

        // serializable test
        Supplier<Boolean> clone = ClonerUtil.clone(supplier);
        Boolean clonedExtract = clone.extract(values);
        assertEquals(true, clonedExtract);
    }

    @Test
    public void containsTest(){
        // value test
        Supplier<Boolean> supplier = contains("str", stringChannel("string"));
        Boolean extract = supplier.extract(values);
        assertTrue(extract);

        assertTrue(supplier.getChannelIdentifiers().contains("string"));

        // null test
        Supplier<Boolean> nullSupplier = contains("str", stringChannel("null"));
        extract = nullSupplier.extract(values);
        Assert.assertNull(extract);

        // serializable test
        Supplier<Boolean> clone = ClonerUtil.clone(supplier);
        Boolean clonedExtract = clone.extract(values);
        assertEquals(true, clonedExtract);
    }

    @Test
    public void lengthTest(){
        // value test
        Supplier<Long> supplier = length(stringChannel("string"));
        Long extract = supplier.extract(values);
        assertEquals(6L, (long)extract);

        assertTrue(supplier.getChannelIdentifiers().contains("string"));

        // null test
        Supplier<Long> nullSupplier = length(stringChannel("null"));
        extract = nullSupplier.extract(values);
        Assert.assertNull(extract);

        // serializable test
        Supplier<Long> clone = ClonerUtil.clone(supplier);
        Long clonedExtract = clone.extract(values);
        assertEquals(6L, (long)clonedExtract);
    }

    @Test
    public void comparatorEquals() {
        Supplier<Boolean> supplier = Suppliers.Comparators.equals(0.1D, doubleChannel("double"));
        boolean extract = supplier.extract(values);
        assertTrue(extract);

        assertTrue(supplier.getChannelIdentifiers().contains("double"));

        // serializable test
        Supplier<Boolean> clone = ClonerUtil.clone(supplier);
        Boolean clonedExtract = clone.extract(values);
        assertEquals(true, clonedExtract);
    }

    @Test
    public void comparatorEquals2() {
        Supplier<Boolean> supplier = Suppliers.Comparators.equals(doubleChannel("double"), doubleChannel("double"));
        boolean extract = supplier.extract(values);
        assertTrue(extract);

        assertTrue(supplier.getChannelIdentifiers().contains("double"));

        // serializable test
        Supplier<Boolean> clone = ClonerUtil.clone(supplier);
        Boolean clonedExtract = clone.extract(values);
        assertEquals(true, clonedExtract);
    }

    @Test
    public void comparatorCompare() {
        Supplier<Long> supplier = Suppliers.Comparators.compare(0.1D, doubleChannel("double"));
        Long extract = supplier.extract(values);
        assertEquals(0L, (long)extract);

        assertTrue(supplier.getChannelIdentifiers().contains("double"));

        // serializable test
        Supplier<Long> clone = ClonerUtil.clone(supplier);
        Long clonedExtract = clone.extract(values);
        assertEquals(0L, (long)clonedExtract);
    }

    @Test
    public void comparatorCompare2() {
        Supplier<Long> supplier = Suppliers.Comparators.compare(doubleChannel("double"), doubleChannel("double"));
        Long extract = supplier.extract(values);
        assertEquals(0L, (long)extract);

        assertTrue(supplier.getChannelIdentifiers().contains("double"));

        // serializable test
        Supplier<Long> clone = ClonerUtil.clone(supplier);
        Long clonedExtract = clone.extract(values);
        assertEquals(0L, (long)clonedExtract);
    }

    @Test
    public void lowerThan1() {
        Supplier<Boolean> supplier = Suppliers.Comparators.lowerThan(-0.1D, doubleChannel("double"));
        Boolean extract = supplier.extract(values);
        assertTrue(extract);

        assertTrue(supplier.getChannelIdentifiers().contains("double"));

        // serializable test
        Supplier<Boolean> clone = ClonerUtil.clone(supplier);
        Boolean clonedExtract = clone.extract(values);
        assertEquals(true, clonedExtract);
    }

    @Test
    public void lowerThan2() {
        Supplier<Boolean> supplier = Suppliers.Comparators.lowerThan(doubleChannel("double2"), doubleChannel("double"));
        Boolean extract = supplier.extract(values);
        assertTrue(extract);

        assertTrue(supplier.getChannelIdentifiers().contains("double"));

        // serializable test
        Supplier<Boolean> clone = ClonerUtil.clone(supplier);
        Boolean clonedExtract = clone.extract(values);
        assertEquals(true, clonedExtract);
    }

    @Test
    public void lowerThanEquals1(){
        Supplier<Boolean> supplier = Suppliers.Comparators.lowerThanEquals(0.1D, doubleChannel("double"));
        Boolean extract = supplier.extract(values);
        assertTrue(extract);

        assertTrue(supplier.getChannelIdentifiers().contains("double"));

        // serializable test
        Supplier<Boolean> clone = ClonerUtil.clone(supplier);
        Boolean clonedExtract = clone.extract(values);
        assertEquals(true, clonedExtract);
    }

    @Test
    public void lowerThanEquals2(){
        Supplier<Boolean> supplier = Suppliers.Comparators.lowerThanEquals(doubleChannel("double"), doubleChannel("double"));
        Boolean extract = supplier.extract(values);
        assertTrue(extract);

        assertTrue(supplier.getChannelIdentifiers().contains("double"));

        // serializable test
        Supplier<Boolean> clone = ClonerUtil.clone(supplier);
        Boolean clonedExtract = clone.extract(values);
        assertEquals(true, clonedExtract);
    }

    @Test
    public void greaterThan1() {
        Supplier<Boolean> supplier = Suppliers.Comparators.greaterThan(0.1D, doubleChannel("double2"));
        Boolean extract = supplier.extract(values);
        assertTrue(extract);

        assertTrue(supplier.getChannelIdentifiers().contains("double2"));

        // serializable test
        Supplier<Boolean> clone = ClonerUtil.clone(supplier);
        Boolean clonedExtract = clone.extract(values);
        assertEquals(true, clonedExtract);
    }

    @Test
    public void greaterThan2() {
        Supplier<Boolean> supplier = Suppliers.Comparators.greaterThan(doubleChannel("double"), doubleChannel("double2"));
        Boolean extract = supplier.extract(values);
        assertTrue(extract);

        assertTrue(supplier.getChannelIdentifiers().contains("double"));

        // serializable test
        Supplier<Boolean> clone = ClonerUtil.clone(supplier);
        Boolean clonedExtract = clone.extract(values);
        assertEquals(true, clonedExtract);
    }

    @Test
    public void greaterThanEquals1(){
        Supplier<Boolean> supplier = Suppliers.Comparators.greaterThanEquals(0.1D, doubleChannel("double"));
        Boolean extract = supplier.extract(values);
        assertTrue(extract);

        assertTrue(supplier.getChannelIdentifiers().contains("double"));

        // serializable test
        Supplier<Boolean> clone = ClonerUtil.clone(supplier);
        Boolean clonedExtract = clone.extract(values);
        assertEquals(true, clonedExtract);
    }

    @Test
    public void greaterThanEquals2(){
        Supplier<Boolean> supplier = Suppliers.Comparators.greaterThanEquals(doubleChannel("double"), doubleChannel("double"));
        Boolean extract = supplier.extract(values);
        assertTrue(extract);

        assertTrue(supplier.getChannelIdentifiers().contains("double"));

        // serializable test
        Supplier<Boolean> clone = ClonerUtil.clone(supplier);
        Boolean clonedExtract = clone.extract(values);
        assertEquals(true, clonedExtract);
    }

    @Test
    public void castToLong(){
        Supplier<Long> supplier = Suppliers.Caster.castToLong(doubleChannel("double"));
        Long extract = supplier.extract(values);
        assertEquals(0L, (long)extract);

        assertTrue(supplier.getChannelIdentifiers().contains("double"));

        // serializable test
        Supplier<Long> clone = ClonerUtil.clone(supplier);
        Long clonedExtract = clone.extract(values);
        assertEquals(0L, (long)clonedExtract);
    }

    @Test
    public void castToDouble(){
        Supplier<Double> supplier = Suppliers.Caster.castToDouble(longChannel("long"));
        Double extract = supplier.extract(values);
        assertEquals(123D, extract, 0.00001);

        assertTrue(supplier.getChannelIdentifiers().contains("long"));

        // serializable test
        Supplier<Double> clone = ClonerUtil.clone(supplier);
        Double clonedExtract = clone.extract(values);
        assertEquals(123D, clonedExtract, 0.0001);
    }

    @Test
    public void castToString(){
        Supplier<String> supplier = Suppliers.Caster.castToString(longChannel("long"));
        String extract = supplier.extract(values);
        assertEquals("123", extract);

        assertTrue(supplier.getChannelIdentifiers().contains("long"));

        // serializable test
        Supplier<String> clone = ClonerUtil.clone(supplier);
        String clonedExtract = clone.extract(values);
        assertEquals("123", clonedExtract);
    }

    @Test
    public void parseLong() {
        Supplier<Long> supplier = Suppliers.Parser.parseLong(stringChannel("stringLong"));
        long extract = supplier.extract(values);
        assertEquals(123L, extract);

        assertTrue(supplier.getChannelIdentifiers().contains("stringLong"));

        // serializable test
        Supplier<Long> clone = ClonerUtil.clone(supplier);
        Long clonedExtract = clone.extract(values);
        assertEquals(123L, (long)clonedExtract);
    }

    @Test
    public void parseDouble() {
        Supplier<Double> supplier = Suppliers.Parser.parseDouble(stringChannel("stringDouble"));
        double extract = supplier.extract(values);
        assertEquals(0.1D, extract, 0.00001);

        assertTrue(supplier.getChannelIdentifiers().contains("stringDouble"));

        // serializable test
        Supplier<Double> clone = ClonerUtil.clone(supplier);
        Double clonedExtract = clone.extract(values);
        assertEquals(0.1D, clonedExtract, 0.00001);
    }

    @Test
    public void add() {
        Supplier<Double> supplier = Suppliers.Mathematics.add(1, doubleChannel("double"));
        double extract = supplier.extract(values);
        assertEquals(1.1D, extract, 0.00001);

        assertTrue(supplier.getChannelIdentifiers().contains("double"));

        supplier = Suppliers.Mathematics.add(doubleChannel("long"), 0.4F);
        extract = supplier.extract(values);
        assertEquals(123.4D, extract, 0.00001);

        supplier = Suppliers.Mathematics.add(doubleChannel("double"), longChannel("long"));
        extract = supplier.extract(values);
        assertEquals(123.1D, extract, 0.00001);

        // serializable test
        Supplier<Double> clone = ClonerUtil.clone(supplier);
        Double clonedExtract = clone.extract(values);
        assertEquals(123.1D, clonedExtract, 0.00001);
    }

    @Test
    public void subtract() {
        Supplier<Double> supplier = Suppliers.Mathematics.subtract(1, doubleChannel("double"));
        double extract = supplier.extract(values);
        assertEquals(0.9D, extract, 0.00001);

        assertTrue(supplier.getChannelIdentifiers().contains("double"));

        supplier = Suppliers.Mathematics.subtract(doubleChannel("long"), 0.4F);
        extract = supplier.extract(values);
        assertEquals(122.6D, extract, 0.00001);

        supplier = Suppliers.Mathematics.subtract(doubleChannel("double"), longChannel("long"));
        extract = supplier.extract(values);
        assertEquals(-122.9D, extract, 0.00001);

        // serializable test
        Supplier<Double> clone = ClonerUtil.clone(supplier);
        Double clonedExtract = clone.extract(values);
        assertEquals(-122.9D, clonedExtract, 0.00001);
    }

    @Test
    public void multiply() {
        Supplier<Double> supplier = Suppliers.Mathematics.multiply(1, doubleChannel("double"));
        double extract = supplier.extract(values);
        assertEquals(0.1D, extract, 0.00001);

        assertTrue(supplier.getChannelIdentifiers().contains("double"));

        supplier = Suppliers.Mathematics.multiply(doubleChannel("long"), 0.4F);
        extract = supplier.extract(values);
        assertEquals(49.2D, extract, 0.00001);

        supplier = Suppliers.Mathematics.multiply(doubleChannel("double"), longChannel("long"));
        extract = supplier.extract(values);
        assertEquals(12.3D, extract, 0.00001);

        // serializable test
        Supplier<Double> clone = ClonerUtil.clone(supplier);
        Double clonedExtract = clone.extract(values);
        assertEquals(12.3D, clonedExtract, 0.00001);
    }

    @Test
    public void divide() {
        Supplier<Double> supplier = Suppliers.Mathematics.divide(1, doubleChannel("double"));
        double extract = supplier.extract(values);
        assertEquals(10D, extract, 0.00001);

        assertTrue(supplier.getChannelIdentifiers().contains("double"));

        supplier = Suppliers.Mathematics.divide(doubleChannel("long"), 2F);
        extract = supplier.extract(values);
        assertEquals(61.5D, extract, 0.00001);

        supplier = Suppliers.Mathematics.divide(longChannel("long"), doubleChannel("double"));
        extract = supplier.extract(values);
        assertEquals(1230D, extract, 0.00001);

        // serializable test
        Supplier<Double> clone = ClonerUtil.clone(supplier);
        Double clonedExtract = clone.extract(values);
        assertEquals(1230D, clonedExtract, 0.00001);
    }

}