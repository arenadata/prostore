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
package io.arenadata.dtm.query.execution.core.service.dml.impl;

import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.dto.QueryParserResponse;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.calcite.core.util.CalciteUtil;
import io.arenadata.dtm.query.execution.core.service.dml.ColumnMetadataService;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ColumnMetadataServiceImpl implements ColumnMetadataService {
    private final QueryParserService parserService;

    public ColumnMetadataServiceImpl(@Qualifier("coreCalciteDMLQueryParserService") QueryParserService parserService) {
        this.parserService = parserService;
    }

    @Override
    public void getColumnMetadata(QueryParserRequest request, Handler<AsyncResult<List<ColumnMetadata>>> handler) {
        parserService.parse(request, ar -> {
            if (ar.succeeded()) {
                QueryParserResponse parserResponse = ar.result();
                try {
                    handler.handle(Future.succeededFuture(getColumnMetadata(parserResponse.getRelNode())));
                } catch (Exception ex) {
                    log.error("Column meta data extract error: ", ex);
                    handler.handle(Future.failedFuture(ex));
                }
            } else {
                log.error("Query parsing error: ", ar.cause());
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    private List<ColumnMetadata> getColumnMetadata(RelRoot relNode) {
        return relNode.project().getRowType().getFieldList().stream()
            .sorted(Comparator.comparing(RelDataTypeField::getIndex))
            .map(f -> new ColumnMetadata(f.getName(), getType(f.getType())))
            .collect(Collectors.toList());
    }

    private ColumnType getType(RelDataType type) {
        return CalciteUtil.toColumnType(type.getSqlTypeName());
    }
}
