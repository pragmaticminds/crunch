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
 * Entity for Double Value.
 * <p>
 * Taken from the CRUNCH Project.
 *
 * @author julian
 */
@ToString
@Entity
@DiscriminatorValue("F")
@EqualsAndHashCode
public class DoubleValue extends Value implements Serializable {

    @Column(name = "VALUE_DOUBLE")
    private Double value;

    public DoubleValue(Double value) {
        this.value = value;
    }

    public DoubleValue() {
        super();
    }

    @Override
    public Object getAsObject() {
        return value;
    }

    @Override
    public Double getAsDouble() {
        return this.value;
    }

    @Override
    public void setAsDouble(Double value) {
        this.value = value;
    }

    @Override
    public <T> T accept(ValueVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public DoubleValue copy() {
        return new DoubleValue(this.value);
    }
}
