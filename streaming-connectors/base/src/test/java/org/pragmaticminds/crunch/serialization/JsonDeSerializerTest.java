package org.pragmaticminds.crunch.serialization;

import org.junit.Test;

import java.util.Objects;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link JsonDeserializer} and {@link JsonSerializer}
 *
 * @author julian
 * Created by julian on 16.08.18
 */
public class JsonDeSerializerTest {

    public static final MyPojo POJO = new MyPojo("a", 1);
    public static final String POJO_STRING = "{\"field1\":\"a\",\"field2\":1}";

    @Test
    public void serialize() {
        byte[] bytes;
        try (JsonSerializer<MyPojo> serializer = new JsonSerializer<>()) {
            bytes = serializer.serialize(POJO);
        }
        assertEquals(POJO_STRING, new String(bytes));
    }

    @Test
    public void deserialize() {
        MyPojo pojo;
        try (JsonDeserializer<MyPojo> deserializer = new JsonDeserializer<>(MyPojo.class)) {
            pojo = deserializer.deserialize(POJO_STRING.getBytes());
        }
        assertEquals(POJO, pojo);
    }

    /**
     * Test Pojo
     */
    private static class MyPojo {

        private String field1;
        private int field2;

        public MyPojo() {
            // For Jackson
        }

        MyPojo(String field1, int field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        public String getField1() {
            return field1;
        }

        public int getField2() {
            return field2;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MyPojo myPojo = (MyPojo) o;
            return field2 == myPojo.field2 &&
                    Objects.equals(field1, myPojo.field1);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field1, field2);
        }
    }
}