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
package io.arenadata.dtm.query.execution.plugin.adb.synchronize.factory.impl;

import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.query.execution.plugin.adb.synchronize.factory.SynchronizeSqlFactory;
import io.arenadata.dtm.query.execution.plugin.api.service.shared.adg.AdgSharedService;
import lombok.val;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.stream.Collectors;

import static io.arenadata.dtm.query.execution.plugin.adb.base.utils.AdbTypeUtil.adbTypeFromDtmType;

@Service
public class AdgSynchronizeSqlFactory implements SynchronizeSqlFactory {
    private static final String DROP_EXTERNAL_TABLE = "DROP EXTERNAL TABLE IF EXISTS %s.TARANTOOL_EXT_%s";
    private static final String CREATE_EXTERNAL_TABLE = "CREATE WRITABLE EXTERNAL TABLE %s.TARANTOOL_EXT_%s\n" +
            "(%s) LOCATION ('pxf://%s?PROFILE=tarantool-upsert&TARANTOOL_SERVER=%s&USER=%s&PASSWORD=%s&TIMEOUT_CONNECT=%d&TIMEOUT_READ=%d&TIMEOUT_REQUEST=%d')\n" +
            "FORMAT 'CUSTOM' (FORMATTER = 'pxfwritable_export')";
    private static final String INSERT_INTO_EXTERNAL_TABLE = "INSERT INTO %s.TARANTOOL_EXT_%s %s";
    private static final String INSERT_INTO_EXTERNAL_TABLE_ONLY_PK = "INSERT INTO %s.TARANTOOL_EXT_%s (%s) %s";
    private static final String ADDITIONAL_FIELD_TO_ONLY_PK = ", sys_op";
    private static final String DELIMETER = ", ";
    private final AdgSharedService adgSharedService;

    public AdgSynchronizeSqlFactory(AdgSharedService adgSharedService) {
        this.adgSharedService = adgSharedService;
    }

    @Override
    public String createExternalTable(String env, String datamart, Entity matView) {
        val spaceName = getSpaceName(env, datamart, matView);
        val columns = getColumns(matView);
        val sharedProperties = adgSharedService.getSharedProperties();
        return String.format(CREATE_EXTERNAL_TABLE, datamart, matView.getName(),
                columns, spaceName, sharedProperties.getServer(), sharedProperties.getUser(), sharedProperties.getPassword(),
                sharedProperties.getConnectTimeout(), sharedProperties.getReadTimeout(), sharedProperties.getRequestTimeout());
    }

    @Override
    public String dropExternalTable(String datamart, Entity matView) {
        return String.format(DROP_EXTERNAL_TABLE, datamart, matView.getName());
    }

    @Override
    public String insertIntoExternalTable(String datamart, Entity matView, String query, boolean onlyPrimaryKeys) {
        if (!onlyPrimaryKeys) {
            return String.format(INSERT_INTO_EXTERNAL_TABLE, datamart, matView.getName(), query);
        }

        String columns = matView.getFields().stream()
                .filter(field -> field.getPrimaryOrder() != null)
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .map(EntityField::getName)
                .collect(Collectors.joining(DELIMETER, "", ADDITIONAL_FIELD_TO_ONLY_PK));

        return String.format(INSERT_INTO_EXTERNAL_TABLE_ONLY_PK, datamart, matView.getName(), columns, query);
    }

    private String getSpaceName(String env, String datamart, Entity matView) {
        return String.format("%s__%s__%s_staging", env, datamart, matView.getName());
    }

    private String getColumns(Entity matView) {
        val builder = new StringBuilder();
        val fields = matView.getFields().stream()
                .sorted(Comparator.comparingInt(EntityField::getOrdinalPosition))
                .collect(Collectors.toList());
        for (int i = 0; i < fields.size(); i++) {
            val field = fields.get(i);
            if (i > 0) {
                builder.append(',');
            }
            builder.append(field.getName()).append(' ').append(mapType(field.getType()));
        }

        builder.append(",sys_op ").append(mapType(ColumnType.BIGINT));
        builder.append(",bucket_id ").append(mapType(ColumnType.BIGINT));
        return builder.toString();
    }

    private String mapType(ColumnType type) {
        switch (type) {
            case VARCHAR:
            case CHAR:
            case UUID:
            case LINK:
                return adbTypeFromDtmType(ColumnType.VARCHAR, null);
            case BIGINT:
            case INT:
            case DATE:
            case TIME:
            case TIMESTAMP:
                return adbTypeFromDtmType(ColumnType.BIGINT, null);
            case BOOLEAN:
            case INT32:
            case DOUBLE:
            case FLOAT:
                return adbTypeFromDtmType(type, null);
            default:
                throw new DtmException("Could not map type to external table type: " + type.name());
        }
    }
}
