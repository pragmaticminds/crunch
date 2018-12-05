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
import java.time.Instant;
import java.util.Date;

/**
 * Entity for Long Value.
 * <p>
 * Taken from the CRUNCH Project.
 *
 * @author julian
 */
@Entity
@DiscriminatorValue("L")
@ToString
@EqualsAndHashCode
public class LongValue extends Value implements Serializable {

    @Column(name = "VALUE_LONG")
    private Long value;

    public LongValue(Long value) {
        this.value = value;
    }

    public LongValue() {
        super();
    }

    @Override
    public Object getAsObject() {
        return value;
    }

    @Override
    public Long getAsLong() {
        return value;
    }

    @Override
    public void setAsLong(Long value) {
        this.value = value;
    }

    @Override
    public String getAsString() {
        return Long.toString(value);
    }

    @Override
    public Double getAsDouble() {
        return (double) value;
    }

    @Override
    public Date getAsDate() {
        return Date.from(Instant.ofEpochMilli(this.value));
    }

    @Override
    public <T> T accept(ValueVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public Value copy() {
        return new LongValue(this.value);
    }
}
