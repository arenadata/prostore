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
package io.arenadata.dtm.common.schema.codec.conversion;

import io.arenadata.dtm.common.schema.codec.type.LocalDateLogicalType;
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

import java.time.LocalDate;

public class LocalDateConversion extends Conversion<LocalDate> {

    private LocalDateConversion() {
        super();
    }

    public static LocalDateConversion getInstance() {
        return LocalDateConversion.LocalDateConversionHolder.INSTANCE;
    }

    @Override
    public Class<LocalDate> getConvertedType() {
        return LocalDate.class;
    }

    @Override
    public String getLogicalTypeName() {
        return LocalDateLogicalType.INSTANCE.getName();
    }

    @Override
    public Schema getRecommendedSchema() {
        return LocalDateLogicalType.INSTANCE.addToSchema(Schema.create(Schema.Type.LONG));
    }

    @Override
    public LocalDate fromLong(Long value, Schema schema, LogicalType type) {
        return LocalDate.ofEpochDay(value);
    }

    @Override
    public Long toLong(LocalDate value, Schema schema, LogicalType type) {
        return value.toEpochDay();
    }

    @Override
    public Integer toInt(LocalDate value, Schema schema, LogicalType type) {
        return (int) value.toEpochDay();
    }

    @Override
    public LocalDate fromInt(Integer value, Schema schema, LogicalType type) {
        return LocalDate.ofEpochDay(value.longValue());
    }

    @Override
    public LocalDate fromCharSequence(CharSequence value, Schema schema, LogicalType type) {
        return LocalDate.parse(value);
    }

    @Override
    public CharSequence toCharSequence(LocalDate value, Schema schema, LogicalType type) {
        return value.toString();
    }

    private static class LocalDateConversionHolder {
        private static final LocalDateConversion INSTANCE = new LocalDateConversion();
    }
}
