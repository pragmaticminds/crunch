package org.pragmaticminds.crunch.serialization;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 03.09.2018
 */
public class JsonDeserializerWrapperTest {
    
    private JsonDeserializerWrapper jsonDeserializerWrapper;
    private JsonSerializer<String> jsonSerializer;
    private byte[] data;
    private String thisIsATestCall;
    
    @Before
    public void setUp() throws Exception {
        jsonDeserializerWrapper = new JsonDeserializerWrapper(String.class);
        jsonSerializer = new JsonSerializer<>();
        thisIsATestCall = "this is a test call";
        data = jsonSerializer.serialize(thisIsATestCall);
    }
    
    @Test
    public void configure() {
        // does nothing -> nothing should happen
        jsonDeserializerWrapper.configure(null, false);
    }
    
    @Test
    public void deserialize() {
        String result = (String) jsonDeserializerWrapper.deserialize("topic0815", data);
        assertEquals(thisIsATestCall, result);
    }
    
    @After
    public void close() {
        jsonDeserializerWrapper.close();
    }
}