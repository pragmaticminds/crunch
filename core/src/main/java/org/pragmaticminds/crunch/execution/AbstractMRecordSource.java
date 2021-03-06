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

package org.pragmaticminds.crunch.execution;

/**
 * Abstract implementation of {@link MRecordSource} which overrides init and close with empty methods.
 *
 * @author julian
 * Created by julian on 15.08.18
 */
public abstract class AbstractMRecordSource implements MRecordSource {

    private Kind kind;

    public AbstractMRecordSource(Kind kind) {
        this.kind = kind;
    }

    @Override
    public Kind getKind() {
        return kind;
    }

    @Override
    public void init() {
        // do nothing
    }

    @Override
    public void close() {
        // do nothing
    }
}
