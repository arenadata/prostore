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
package io.arenadata.dtm.query.execution.core.ddl.service;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlLogicalCall;
import io.arenadata.dtm.query.execution.core.base.repository.ServiceDbFacade;
import io.arenadata.dtm.query.execution.core.base.service.metadata.MetadataExecutor;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.vertx.core.Future;
import lombok.AllArgsConstructor;
import lombok.val;

@AllArgsConstructor
public abstract class QueryResultDdlExecutor implements DdlExecutor<QueryResult> {
    protected final MetadataExecutor<DdlRequestContext> metadataExecutor;
    protected final ServiceDbFacade serviceDbFacade;

    protected QueryRequest replaceDatabaseInSql(QueryRequest request) {
        String sql = request.getSql().replaceAll("(?i) database", " schema");
        request.setSql(sql);
        return request;
    }

    protected String getSchemaName(String requestDatamart, String sqlNodeName) {
        int indexComma = sqlNodeName.indexOf(".");
        return indexComma == -1 ? requestDatamart : sqlNodeName.substring(0, indexComma);
    }

    protected String getTableName(String sqlNodeName) {
        int indexComma = sqlNodeName.indexOf(".");
        return sqlNodeName.substring(indexComma + 1);
    }

    protected String getTableNameWithSchema(String schema, String tableName) {
        return schema + "." + tableName;
    }

    protected Future<Void> executeRequest(DdlRequestContext context) {
        val node = context.getSqlNode();

        if (node instanceof SqlLogicalCall && ((SqlLogicalCall) node).isLogicalOnly()) {
            return Future.succeededFuture();
        }

        return metadataExecutor.execute(context);
    }
}
