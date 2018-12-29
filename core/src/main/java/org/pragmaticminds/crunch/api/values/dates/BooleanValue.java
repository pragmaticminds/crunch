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

package org.pragmaticminds.crunch.api.values.dates;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.persistence.Column;
import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;
import java.io.Serializable;

/**
 * Entity for Boolean Value.
 * <p>
 * Taken from the CRUNCH Project.
 *
 * @author julian
 */
@Entity
@DiscriminatorValue("B")
@ToString
@EqualsAndHashCode
public class BooleanValue extends Value implements Serializable {

    @Column(name = "VALUE_BOOL")
    private Boolean value;

    public BooleanValue(Boolean value) {
        this.value = value;
    }

    public BooleanValue() {
        super();
    }

    @Override
    public Object getAsObject() {
        return value;
    }

    @Override
    public boolean getAsBoolean() {
        return (Boolean) getAsObject();
    }

    @Override
    public void setAsBoolean(boolean value) {
        this.value = value;
    }

    @Override
    public <T> T accept(ValueVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public BooleanValue copy() {
        return new BooleanValue(this.value);
    }
}
