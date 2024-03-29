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
package io.arenadata.dtm.query.execution.plugin.adp.enrichment.service;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.SchemaExtender;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adp.base.Constants.*;


@Service("adpSchemaExtender")
public class AdpSchemaExtender implements SchemaExtender {

    @Override
    public Datamart createPhysicalSchema(Datamart logicalSchema, String systemName) {
        Datamart extendedSchema = new Datamart();
        extendedSchema.setMnemonic(logicalSchema.getMnemonic());
        List<Entity> extendedEntities = new ArrayList<>();
        logicalSchema.getEntities().forEach(entity -> {
            val extendedEntity = entity.copy();
            val extendedEntityFields = new ArrayList<>(extendedEntity.getFields());
            extendedEntityFields.addAll(getExtendedColumns());
            extendedEntity.setFields(extendedEntityFields);
            extendedEntities.add(extendedEntity);
            extendedEntities.add(getExtendedSchema(extendedEntity, "_".concat(HISTORY_TABLE)));
            extendedEntities.add(getExtendedSchema(extendedEntity, "_".concat(STAGING_TABLE)));
            extendedEntities.add(getExtendedSchema(extendedEntity, "_".concat(ACTUAL_TABLE)));
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
