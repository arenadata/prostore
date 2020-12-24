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
package io.arenadata.dtm.query.execution.core.service.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityFieldUtils;
import io.arenadata.dtm.query.execution.core.service.DdlQueryGenerator;
import lombok.val;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class HsqlDdlQueryGenerator implements DdlQueryGenerator {

    public static final String DELIMITER = ", ";

    @Override
    public String generateCreateViewQuery(Entity entity) {
        val tableName = entity.getNameWithSchema();
        val sb = new StringBuilder()
                .append("CREATE VIEW ").append(tableName)
                .append(" AS ")
                .append(entity.getViewQuery());
        return sb.toString();
    }

    @Override
    public String generateCreateTableQuery(Entity entity) {
        val tableName = entity.getNameWithSchema();
        val sb = new StringBuilder()
                .append("CREATE TABLE ").append(tableName)
                .append(" (");
        appendClassTableFields(sb, entity.getFields());

        List<EntityField> pkList = EntityFieldUtils.getPrimaryKeyList(entity.getFields());
        if (pkList.size() > 0) {
            appendPrimaryKeys(sb, tableName, pkList);
        }
        sb.append(")");
        return sb.toString();
    }

    private void appendClassTableFields(StringBuilder builder, List<EntityField> fields) {
        val columns = fields.stream()
                .map(this::getColumnDDLByField)
                .collect(Collectors.joining(DELIMITER));
        builder.append(columns);
    }

    private String getColumnDDLByField(EntityField field) {
        val sb = new StringBuilder();
        sb.append(field.getName())
                .append(" ")
                .append(getColumnType(field))
                .append(" ");
        if (!field.getNullable()) {
            sb.append("NOT NULL");
        }
        return sb.toString();
    }

    private String getColumnType(EntityField field) {
        switch (field.getType()) {
            case INT:
                return "INTEGER";
            case UUID:
                return "VARCHAR(36)";
            case CHAR:
            case VARCHAR:
            case TIME:
            case TIMESTAMP:
                return getFieldTypeWithSize(field);
            default:
                return field.getType().toString();
        }
    }

    private String getFieldTypeWithSize(EntityField field) {
        val size = field.getSize() == null ? "" : "(" + field.getSize() + ")";
        return field.getType().toString() + size;
    }

    private void appendPrimaryKeys(StringBuilder builder, String tableName, Collection<EntityField> pkList) {
        List<String> pkFields = pkList.stream().map(EntityField::getName).collect(Collectors.toList());
        builder.append(DELIMITER)
                .append("constraint ")
                .append("pk_")
                .append(tableName.replace('.', '_'))
                .append(" primary key (")
                .append(String.join(DELIMITER, pkFields))
                .append(")");
    }
}
