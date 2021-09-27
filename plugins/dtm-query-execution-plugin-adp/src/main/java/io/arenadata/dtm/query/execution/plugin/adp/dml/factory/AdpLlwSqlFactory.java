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
package io.arenadata.dtm.query.execution.plugin.adp.dml.factory;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityFieldUtils;
import lombok.val;

import java.util.stream.Collectors;

public final class AdpLlwSqlFactory {

    private static final String LLW_DELETE_SQL_TEMPLATE = "INSERT INTO %s.%s_staging (%s,sys_op) %s";

    private AdpLlwSqlFactory() {
    }

    public static String createLlwDeleteSql(String enrichedSelect, String datamart, Entity entity) {
        val columns = EntityFieldUtils.getNotNullableFields(entity).stream()
                .map(EntityField::getName)
                .collect(Collectors.joining(","));
        return String.format(LLW_DELETE_SQL_TEMPLATE, datamart, entity.getName(), columns, enrichedSelect);
    }
}
