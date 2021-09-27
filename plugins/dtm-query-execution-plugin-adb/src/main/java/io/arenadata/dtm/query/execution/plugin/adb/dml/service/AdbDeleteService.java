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
package io.arenadata.dtm.query.execution.plugin.adb.dml.service;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.delta.DeltaType;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.common.model.ddl.EntityFieldUtils;
import io.arenadata.dtm.query.calcite.core.extension.dml.SqlSelectExt;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.calcite.core.util.SqlNodeTemplates;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adb.dml.factory.AdbDeleteSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.dto.TransferDataRequest;
import io.arenadata.dtm.query.execution.plugin.adb.mppw.kafka.service.executor.AdbMppwDataTransferService;
import io.arenadata.dtm.query.execution.plugin.adb.query.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.api.dml.LlwUtils;
import io.arenadata.dtm.query.execution.plugin.api.request.DeleteRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.DeleteService;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.enrichment.service.QueryEnrichmentService;
import io.vertx.core.Future;
import lombok.val;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@Service("adbDeleteService")
public class AdbDeleteService implements DeleteService {
    private static final SqlLiteral ONE_SYS_OP = SqlNodeTemplates.longLiteral(1);
    private static final List<SqlLiteral> SYSTEM_LITERALS = singletonList(ONE_SYS_OP);
    private final DatabaseExecutor executor;
    private final AdbMppwDataTransferService dataTransferService;
    private final QueryEnrichmentService queryEnrichmentService;
    private final QueryParserService queryParserService;
    private final SqlDialect sqlDialect;

    public AdbDeleteService(DatabaseExecutor executor,
                            AdbMppwDataTransferService dataTransferService,
                            @Qualifier("adbQueryEnrichmentService") QueryEnrichmentService queryEnrichmentService,
                            @Qualifier("adbCalciteDMLQueryParserService") QueryParserService queryParserService,
                            @Qualifier("adbSqlDialect") SqlDialect sqlDialect) {
        this.executor = executor;
        this.dataTransferService = dataTransferService;
        this.queryEnrichmentService = queryEnrichmentService;
        this.queryParserService = queryParserService;
        this.sqlDialect = sqlDialect;
    }

    @Override
    public Future<Void> execute(DeleteRequest request) {
        return Future.future(promise -> {
            val condition = request.getQuery().getCondition();
            val tableIdentifier = LlwUtils.getTableIdentifier(request.getDatamartMnemonic(), request.getEntity().getName(), request.getQuery().getAlias());
            val notNullableFields = EntityFieldUtils.getNotNullableFields(request.getEntity());
            val columns = LlwUtils.getExtendedSelectList(notNullableFields, SYSTEM_LITERALS);
            val sqlSelectExt = new SqlSelectExt(SqlParserPos.ZERO, SqlNodeList.EMPTY, columns, tableIdentifier, condition, null, null, SqlNodeList.EMPTY, null, null, null, SqlNodeList.EMPTY, null, false);
            val schema = request.getDatamarts();
            queryParserService.parse(new QueryParserRequest(sqlSelectExt, schema))
                    .compose(queryParserResponse -> enrichQuery(request, sqlSelectExt, schema, queryParserResponse))
                    .map(LlwUtils::replaceDynamicParams)
                    .map(this::sqlNodeToString)
                    .compose(enrichedQuery -> executeInsert(request, enrichedQuery))
                    .compose(v -> executeTransfer(request))
                    .onComplete(promise);
        });
    }

    private String sqlNodeToString(SqlNode sqlNode) {
        return Util.toLinux(sqlNode.toSqlString(sqlDialect).getSql()).replaceAll("\r\n|\r|\n", " ");
    }

    private Future<Void> executeTransfer(DeleteRequest request) {
        val transferDataRequest = TransferDataRequest.builder()
                .datamart(request.getDatamartMnemonic())
                .hotDelta(request.getSysCn())
                .tableName(request.getEntity().getName())
                .columnList(EntityFieldUtils.getFieldNames(request.getEntity()))
                .keyColumnList(EntityFieldUtils.getPkFieldNames(request.getEntity()))
                .build();
        return dataTransferService.execute(transferDataRequest);
    }

    private Future<Void> executeInsert(DeleteRequest request, String enrichedQuery) {
        val queryToExecute = AdbDeleteSqlFactory.createDeleteSql(enrichedQuery, request.getDatamartMnemonic(), request.getEntity());
        return executor.executeWithParams(queryToExecute, request.getParameters(), emptyList())
                .mapEmpty();
    }

    private Future<SqlNode> enrichQuery(DeleteRequest request, SqlSelectExt sqlSelectExt, List<Datamart> schema, QueryParserResponse queryParserResponse) {
        val enrichRequest = EnrichQueryRequest.builder()
                .query(sqlSelectExt)
                .schema(schema)
                .envName(request.getEnvName())
                .deltaInformations(Arrays.asList(
                        DeltaInformation.builder()
                                .type(DeltaType.WITHOUT_SNAPSHOT)
                                .selectOnNum(request.getDeltaOkSysCn())
                                .build()
                ))
                .build();
        return queryEnrichmentService.getEnrichedSqlNode(enrichRequest, queryParserResponse);
    }


}
