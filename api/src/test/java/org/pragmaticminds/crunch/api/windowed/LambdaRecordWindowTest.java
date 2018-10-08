package org.pragmaticminds.crunch.api.windowed;

import org.junit.Before;
import org.junit.Test;
import org.pragmaticminds.crunch.api.pipe.ClonerUtil;
import org.pragmaticminds.crunch.api.records.MRecord;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 27.09.2018
 */
public class LambdaRecordWindowTest {
    private LambdaRecordWindow window;
    private LambdaRecordWindow clone;
    
    @Before
    public void setUp() throws Exception {
        window = new LambdaRecordWindow(
            record -> true,
            () -> new ArrayList<>(Collections.singletonList("test"))
        );
        clone = ClonerUtil.clone(window);
    }
    
    @Test
    public void inWindow() {
        assertTrue(window.inWindow(mock(MRecord.class)));
        assertTrue(clone.inWindow(mock(MRecord.class)));
    }
    
    @Test
    public void getChannelIdentifiers() {
        assertTrue(window.getChannelIdentifiers().contains("test"));
        assertTrue(clone.getChannelIdentifiers().contains("test"));
    }
}