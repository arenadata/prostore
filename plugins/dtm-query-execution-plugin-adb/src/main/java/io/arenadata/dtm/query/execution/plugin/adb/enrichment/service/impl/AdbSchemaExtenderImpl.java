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
package io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.impl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adb.enrichment.service.SchemaExtender;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adb.base.factory.Constants.*;

/**
 * Implementing a Logic to Physical Conversion
 */
@Service("adbSchemaExtender")
public class AdbSchemaExtenderImpl implements SchemaExtender {

    @Override
    public Datamart createPhysicalSchema(Datamart schema) {
        Datamart extendedSchema = new Datamart();
        extendedSchema.setMnemonic(schema.getMnemonic());
        List<Entity> extendedEntities = new ArrayList<>();
        schema.getEntities().forEach(entity -> {
            val extendedEntityFields = new ArrayList<>(entity.getFields());
            extendedEntityFields.addAll(getExtendedColumns());
            entity.setFields(extendedEntityFields);
            extendedEntities.add(entity);
            extendedEntities.add(getExtendedSchema(entity, "_".concat(HISTORY_TABLE)));
            extendedEntities.add(getExtendedSchema(entity, "_".concat(STAGING_TABLE)));
            extendedEntities.add(getExtendedSchema(entity, "_".concat(ACTUAL_TABLE)));
        });
        extendedSchema.setEntities(extendedEntities);
        return extendedSchema;
    }

    private Entity getExtendedSchema(Entity entity, String tablePostfix) {
        return entity.toBuilder()
                .fields(entity.getFields().stream()
                        .map(ef -> ef.toBuilder().build())
                        .collect(Collectors.toList()))
                .name(entity.getName() + tablePostfix)
                .build();
    }

    private List<EntityField> getExtendedColumns() {
        List<EntityField> tableAttributeList = new ArrayList<>();
        tableAttributeList.add(generateNewField(SYS_OP_ATTR, false));
        tableAttributeList.add(generateNewField(SYS_TO_ATTR, true));
        tableAttributeList.add(generateNewField(SYS_FROM_ATTR, false));
        return tableAttributeList;
    }

    private EntityField generateNewField(String name, boolean isNullable) {
        return EntityField.builder()
                .type(ColumnType.BIGINT)
                .name(name)
                .nullable(isNullable)
                .build();
    }



}
