package org.pragmaticminds.crunch.api.trigger.extractor;

import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;
import org.pragmaticminds.crunch.api.trigger.comparator.Supplier;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.pragmaticminds.crunch.api.trigger.comparator.Suppliers.ChannelExtractors.channel;

/**
 * Only test the factory methods.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 20.09.2018
 */
public class ExtractorsTest {
    
    @Test
    public void allChannelMapExtractor() {
        assertNotNull(Extractors.allChannelMapExtractor());
    }
    
    @Test
    public void channelMapExtractor() {
        assertNotNull(Extractors.channelMapExtractor(channel("test1")));
    }
    
    @Test
    public void channelMapExtractor1() {
        assertNotNull(Extractors.channelMapExtractor(Arrays.asList(channel("test1"))));
    }
    
    @Test
    public void channelMapExtractor2() {
        Map<Supplier, String> map = new HashMap<>();
        map.put(channel("test1"), "t1");
        assertNotNull(Extractors.channelMapExtractor(map));
    
        // serializable test
        assertNotNull(ClonerUtil.clone(Extractors.channelMapExtractor(map)));;
    }
}