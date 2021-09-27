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
package io.arenadata.dtm.query.execution.plugin.adp.db.converter;

import io.arenadata.dtm.common.converter.SqlTypeConverter;
import io.arenadata.dtm.common.converter.transformer.ColumnTransformer;
import io.arenadata.dtm.common.converter.transformer.impl.*;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.query.execution.plugin.adp.db.converter.transformer.AdpBigintFromNumberTransformer;
import io.arenadata.dtm.query.execution.plugin.adp.db.converter.transformer.AdpDoubleFromNumberTransformer;
import io.arenadata.dtm.query.execution.plugin.adp.db.converter.transformer.AdpFloatFromNumberTransformer;
import io.arenadata.dtm.query.execution.plugin.adp.db.converter.transformer.AdpLongFromNumericTransformer;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;


@Component("adpFromSqlConverter")
public class AdpFromSqlConverter implements SqlTypeConverter {
    private final Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap;

    public AdpFromSqlConverter() {
        Map<ColumnType, Map<Class<?>, ColumnTransformer>> transformerMap = new HashMap<>();
        Map<Class<?>, ColumnTransformer> adpLongFromNumericTransformerMap = ColumnTransformer.getTransformerMap(new AdpLongFromNumericTransformer());
        transformerMap.put(ColumnType.INT, adpLongFromNumericTransformerMap);
        transformerMap.put(ColumnType.INT32, adpLongFromNumericTransformerMap);
        Map<Class<?>, ColumnTransformer> varcharFromStringTransformerMap = ColumnTransformer.getTransformerMap(new VarcharFromStringTransformer());
        transformerMap.put(ColumnType.VARCHAR, varcharFromStringTransformerMap);
        transformerMap.put(ColumnType.LINK, varcharFromStringTransformerMap);
        transformerMap.put(ColumnType.CHAR, varcharFromStringTransformerMap);
        transformerMap.put(ColumnType.BIGINT, ColumnTransformer.getTransformerMap(new AdpBigintFromNumberTransformer()));
        transformerMap.put(ColumnType.DOUBLE, ColumnTransformer.getTransformerMap(new AdpDoubleFromNumberTransformer()));
        transformerMap.put(ColumnType.FLOAT, ColumnTransformer.getTransformerMap(new AdpFloatFromNumberTransformer()));
        transformerMap.put(ColumnType.DATE, ColumnTransformer.getTransformerMap(
                new DateFromNumericTransformer(),
                new DateFromLongTransformer(),
                new DateFromLocalDateTransformer()
        ));
        transformerMap.put(ColumnType.TIME, ColumnTransformer.getTransformerMap(new TimeFromLocalTimeTransformer()));
        transformerMap.put(ColumnType.TIMESTAMP, ColumnTransformer.getTransformerMap(new TimestampFromLocalDateTimeTransformer()));
        transformerMap.put(ColumnType.BOOLEAN, ColumnTransformer.getTransformerMap(new BooleanFromBooleanTransformer()));
        transformerMap.put(ColumnType.UUID, ColumnTransformer.getTransformerMap(new UuidFromStringTransformer()));
        transformerMap.put(ColumnType.BLOB, ColumnTransformer.getTransformerMap(new BlobFromObjectTransformer()));
        transformerMap.put(ColumnType.ANY, ColumnTransformer.getTransformerMap(new AnyFromObjectTransformer()));
        this.transformerMap = transformerMap;
    }

    @Override
    public Map<ColumnType, Map<Class<?>, ColumnTransformer>> getTransformerMap() {
        return this.transformerMap;
    }
}
