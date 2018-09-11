package org.pragmaticminds.crunch.source;

import org.junit.Test;
import org.pragmaticminds.crunch.api.records.MRecord;

import java.io.IOException;

import static org.junit.Assert.assertNotNull;

/**
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 11.09.2018
 */
public class FileMRecordSourceIT {
    
    @Test
    public void readFile() throws IOException {
        // create file source
        FileMRecordSource source = new FileMRecordSource("./src/test/resources/LHL_06.json");
        
        // read all values
        while (source.hasRemaining()){
            MRecord mRecord = source.get();
            
            // check if read
            assertNotNull(mRecord);
        }
    }
}