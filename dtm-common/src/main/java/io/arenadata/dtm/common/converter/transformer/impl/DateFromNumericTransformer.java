/*
 * Copyright © 2020 ProStore
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.arenadata.dtm.common.converter.transformer.impl;

import io.arenadata.dtm.common.converter.transformer.AbstractColumnTransformer;
import io.arenadata.dtm.common.model.ddl.ColumnType;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collection;

public class DateFromNumericTransformer extends AbstractColumnTransformer<Date, Number> {

    @Override
    public Date transformValue(Number value) {
        return Date.valueOf(LocalDate.ofEpochDay(value.longValue()));
    }

    @Override
    public Collection<Class<?>> getTransformClasses() {
        return Arrays.asList(Long.class, Integer.class);
    }

    @Override
    public ColumnType getType() {
        return ColumnType.DATE;
    }
}
