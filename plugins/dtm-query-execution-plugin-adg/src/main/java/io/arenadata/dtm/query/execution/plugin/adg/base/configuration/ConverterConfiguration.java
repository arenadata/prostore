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
package io.arenadata.dtm.query.execution.plugin.adg.base.configuration;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.converter.transformer.ColumnTransformer;
import io.arenadata.dtm.common.converter.transformer.impl.*;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

import static io.arenadata.dtm.common.converter.transformer.ColumnTransformer.getTransformerMap;

@Configuration
public class ConverterConfiguration {

    @Bean("adgFromSqlTransformerMap")
    public Map<ColumnType, Map<Class<?>, ColumnTransformer>> adgFromSqlTransformerMap(DtmConfig dtmSettings) {
        Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap = new HashMap<>();
        transformerMap.put(ColumnType.INT, getTransformerMap(new NumberFromLongTransformer()));
        transformerMap.put(ColumnType.VARCHAR, getTransformerMap(new VarcharFromStringTransformer()));
        transformerMap.put(ColumnType.CHAR, transformerMap.get(ColumnType.VARCHAR));
        transformerMap.put(ColumnType.BIGINT, getTransformerMap(new NumberFromBigintTransformer()));
        transformerMap.put(ColumnType.DOUBLE, getTransformerMap(new NumberFromDoubleTransformer()));
        transformerMap.put(ColumnType.FLOAT, getTransformerMap(new NumberFromFloatTransformer()));
        transformerMap.put(ColumnType.DATE, getTransformerMap(new LongDateFromIntTransformer()));
        transformerMap.put(ColumnType.TIME, getTransformerMap(new LongTimeFromLongTransformer()));
        transformerMap.put(ColumnType.TIMESTAMP, getTransformerMap(
                new LongTimestampFromLongTransformer()));
        transformerMap.put(ColumnType.BOOLEAN, getTransformerMap(new BooleanFromBooleanTransformer()));
        transformerMap.put(ColumnType.ANY, getTransformerMap(new AnyFromObjectTransformer()));
        return transformerMap;
    }
}
