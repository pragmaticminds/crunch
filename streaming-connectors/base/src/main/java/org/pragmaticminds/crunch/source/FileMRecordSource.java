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

package org.pragmaticminds.crunch.source;

import org.pragmaticminds.crunch.api.records.MRecord;
import org.pragmaticminds.crunch.api.values.UntypedValues;
import org.pragmaticminds.crunch.execution.AbstractMRecordSource;
import org.pragmaticminds.crunch.execution.MRecordSource;
import org.pragmaticminds.crunch.serialization.JsonDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

/**
 * This class can read serialized {@link UntypedValues} from a File and present them as a {@link MRecordSource}.
 * The records in the file should be separated by new line.
 *
 * @author Erwin Wagasow
 * Created by Erwin Wagasow on 11.09.2018
 */
public class FileMRecordSource extends AbstractMRecordSource implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(FileMRecordSource.class);

    private final transient BufferedReader                  reader;
    private final transient Iterator<String>                iterator;
    private final transient FileReader                      fileReader;
    private final           JsonDeserializer<UntypedValues> deserializer;

    /**
     * Main constructor.
     * Opens a {@link FileReader} and creates a {@link Iterator} on the lines of the file.
     * If a the file of interest is not existing or in a other way damaged, a {@link UncheckedIOException} can be thrown.
     *
     * @param filePath of the serialized UntypedValues data file.
     */
    public FileMRecordSource(String filePath) {
        // set Kind as finite
        super(MRecordSource.Kind.FINITE);

        // initialize file reading structures
        try {
            fileReader = new FileReader(new File(filePath));
            reader = new BufferedReader(fileReader);
            iterator = reader.lines().iterator();
        } catch (IOException ex) {
            logger.error("File could not be read!", ex);
            throw new UncheckedIOException(ex);
        }

        // initialize json parser
        deserializer = new JsonDeserializer<>(UntypedValues.class);
    }

    /**
     * Request the next record.
     * That for the next line is requested from the file iterator and than parsed
     *
     * @return record
     */
    @Override
    public MRecord get() {
        if(iterator.hasNext()){
            String line = iterator.next();
          return deserializer.deserialize(line.getBytes(StandardCharsets.UTF_8));
        }else{
            close();
        }
        return null;
    }


    /**
     * Check whether more records are available for fetch
     *
     * @return true if records can be fetched using {@link #get()}
     */
    @Override
    public boolean hasRemaining() {
        if(iterator.hasNext()){
            return true;
        }else{
            close();
            return false;
        }
    }

    /**
     * Closes the file handle to the source file.
     */
    @Override
    public void close() {
        super.close();
        try {
            reader.close();
            fileReader.close();
        } catch (IOException ex) {
            logger.error("File could not be closed!", ex);
            throw new UncheckedIOException(ex);
        }
    }
}
