/*
 * Copyright © 2021 ProStore
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
package io.arenadata.dtm.query.execution.plugin.adb.base.configuration;

import io.arenadata.dtm.common.converter.transformer.ColumnTransformer;
import io.arenadata.dtm.common.converter.transformer.impl.*;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.query.execution.plugin.adb.base.service.converter.transformer.AdbBigintFromNumberTransformer;
import io.arenadata.dtm.query.execution.plugin.adb.base.service.converter.transformer.AdbDoubleFromNumberTransformer;
import io.arenadata.dtm.query.execution.plugin.adb.base.service.converter.transformer.AdbFloatFromNumberTransformer;
import io.arenadata.dtm.query.execution.plugin.adb.base.service.converter.transformer.AdbLongFromNumericTransformer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

import static io.arenadata.dtm.common.converter.transformer.ColumnTransformer.getTransformerMap;

@Configuration
public class ConverterConfiguration {

    @Bean("adbTransformerMap")
    public Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap() {
        Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap = new HashMap<>();
        Map<Class<?>, ColumnTransformer> adbLongFromNumericTransformerMap = getTransformerMap(new AdbLongFromNumericTransformer());
        transformerMap.put(ColumnType.INT, adbLongFromNumericTransformerMap);
        transformerMap.put(ColumnType.INT32, adbLongFromNumericTransformerMap);
        Map<Class<?>, ColumnTransformer> varcharFromStringTransformerMap = getTransformerMap(new VarcharFromStringTransformer());
        transformerMap.put(ColumnType.VARCHAR, varcharFromStringTransformerMap);
        transformerMap.put(ColumnType.LINK, varcharFromStringTransformerMap);
        transformerMap.put(ColumnType.CHAR, varcharFromStringTransformerMap);
        transformerMap.put(ColumnType.BIGINT, getTransformerMap(new AdbBigintFromNumberTransformer()));
        transformerMap.put(ColumnType.DOUBLE, getTransformerMap(new AdbDoubleFromNumberTransformer()));
        transformerMap.put(ColumnType.FLOAT, getTransformerMap(new AdbFloatFromNumberTransformer()));
        transformerMap.put(ColumnType.DATE, getTransformerMap(
                new DateFromNumericTransformer(),
                new DateFromLongTransformer(),
                new DateFromLocalDateTransformer()
        ));
        transformerMap.put(ColumnType.TIME, getTransformerMap(new TimeFromLocalTimeTransformer()));
        transformerMap.put(ColumnType.TIMESTAMP, getTransformerMap(new TimestampFromLocalDateTimeTransformer()));
        transformerMap.put(ColumnType.BOOLEAN, getTransformerMap(new BooleanFromBooleanTransformer()));
        transformerMap.put(ColumnType.UUID, getTransformerMap(new UuidFromStringTransformer()));
        transformerMap.put(ColumnType.BLOB, getTransformerMap(new BlobFromObjectTransformer()));
        transformerMap.put(ColumnType.ANY, getTransformerMap(new AnyFromObjectTransformer()));
        return transformerMap;
    }

    @Bean("adbFromSqlTransformerMap")
    public Map<ColumnType, Map<Class<?>, ColumnTransformer>> adbFromSqlTransformerMap() {
        Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap = new HashMap<>();
        Map<Class<?>, ColumnTransformer> numberFromLongTransformerMap = getTransformerMap(new NumberFromLongTransformer());
        transformerMap.put(ColumnType.INT, numberFromLongTransformerMap);
        transformerMap.put(ColumnType.INT32, numberFromLongTransformerMap);
        Map<Class<?>, ColumnTransformer> varcharFromStringTransformerMap = getTransformerMap(new VarcharFromStringTransformer());
        transformerMap.put(ColumnType.VARCHAR, varcharFromStringTransformerMap);
        transformerMap.put(ColumnType.CHAR, varcharFromStringTransformerMap);
        transformerMap.put(ColumnType.LINK, varcharFromStringTransformerMap);
        transformerMap.put(ColumnType.UUID, varcharFromStringTransformerMap);
        transformerMap.put(ColumnType.BIGINT, getTransformerMap(new NumberFromBigintTransformer()));
        transformerMap.put(ColumnType.DOUBLE, getTransformerMap(new NumberFromDoubleTransformer()));
        transformerMap.put(ColumnType.FLOAT, getTransformerMap(new NumberFromFloatTransformer()));
        transformerMap.put(ColumnType.DATE, getTransformerMap(new LocalDateFromNumberTransformer()));
        transformerMap.put(ColumnType.TIME, getTransformerMap(new LocalTimeFromNumberTransformer()));
        transformerMap.put(ColumnType.TIMESTAMP, getTransformerMap(new LocalDateTimeFromNumberTransformer()));
        transformerMap.put(ColumnType.BOOLEAN, getTransformerMap(new BooleanFromBooleanTransformer()));
        transformerMap.put(ColumnType.ANY, getTransformerMap(new AnyFromObjectTransformer()));
        return transformerMap;
    }
}
