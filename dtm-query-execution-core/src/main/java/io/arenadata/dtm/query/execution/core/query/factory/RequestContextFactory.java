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
package io.arenadata.dtm.query.execution.core.query.factory;

import io.arenadata.dtm.common.configuration.core.CoreConstants;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.model.RequestStatus;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.common.request.DatamartRequest;
import io.arenadata.dtm.query.calcite.core.extension.check.SqlCheckCall;
import io.arenadata.dtm.query.calcite.core.extension.config.SqlConfigCall;
import io.arenadata.dtm.query.calcite.core.extension.ddl.truncate.SqlBaseTruncate;
import io.arenadata.dtm.query.calcite.core.extension.delta.SqlDeltaCall;
import io.arenadata.dtm.query.calcite.core.extension.dml.SqlDataSourceTypeGetter;
import io.arenadata.dtm.query.execution.core.base.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.core.base.dto.request.CoreRequestContext;
import io.arenadata.dtm.query.execution.core.check.dto.CheckContext;
import io.arenadata.dtm.query.execution.core.config.dto.ConfigRequestContext;
import io.arenadata.dtm.query.execution.core.ddl.dto.DdlRequestContext;
import io.arenadata.dtm.query.execution.core.delta.dto.operation.DeltaRequestContext;
import io.arenadata.dtm.query.execution.core.dml.dto.DmlRequest;
import io.arenadata.dtm.query.execution.core.dml.dto.DmlRequestContext;
import io.arenadata.dtm.query.execution.core.eddl.dto.EddlRequestContext;
import io.arenadata.dtm.query.execution.core.edml.dto.EdmlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.ConfigRequest;
import lombok.val;
import org.apache.calcite.sql.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Optional;

@Component
public class RequestContextFactory {
    private final AppConfiguration coreConfiguration;

    @Autowired
    public RequestContextFactory(AppConfiguration coreConfiguration) {
        this.coreConfiguration = coreConfiguration;
    }

    public CoreRequestContext<? extends DatamartRequest, ? extends SqlNode> create(QueryRequest request,
                                                                                   SqlNode node) {
        val envName = coreConfiguration.getEnvName();
        if (isConfigRequest(node)) {
            return ConfigRequestContext.builder()
                    .request(new ConfigRequest(request))
                    .envName(envName)
                    .metrics(createRequestMetrics(request))
                    .sqlConfigCall((SqlConfigCall) node)
                    .build();
        } else if (isDdlRequest(node)) {
            switch (node.getKind()) {
                case OTHER_DDL:
                    if (node instanceof SqlBaseTruncate) {
                        return new DdlRequestContext(
                                createRequestMetrics(request),
                                new DatamartRequest(request),
                                node,
                                null,
                                envName);
                    } else {
                        return EddlRequestContext.builder()
                                .request(new DatamartRequest(request))
                                .envName(envName)
                                .metrics(createRequestMetrics(request))
                                .sqlNode(node)
                                .build();
                    }
                default:
                    return new DdlRequestContext(
                            createRequestMetrics(request),
                            new DatamartRequest(request),
                            node,
                            null,
                            envName);
            }
        } else if (node instanceof SqlDeltaCall) {
            return new DeltaRequestContext(
                    createRequestMetrics(request),
                    new DatamartRequest(request),
                    envName,
                    (SqlDeltaCall) node);
        } else if (SqlKind.CHECK.equals(node.getKind())) {
            SqlCheckCall sqlCheckCall = (SqlCheckCall) node;
            Optional.ofNullable(sqlCheckCall.getSchema()).ifPresent(request::setDatamartMnemonic);
            return CheckContext.builder()
                    .request(new DatamartRequest(request))
                    .envName(envName)
                    .metrics(createRequestMetrics(request))
                    .checkType(sqlCheckCall.getType())
                    .sqlCheckCall(sqlCheckCall)
                    .build();
        }

        switch (node.getKind()) {
            case INSERT: {
                if (((SqlInsert) node).isUpsert()) {
                    return DmlRequestContext.builder()
                            .request(new DmlRequest(request))
                            .envName(envName)
                            .metrics(createRequestMetrics(request))
                            .sourceType(getDmlSourceType(node))
                            .sqlNode(node)
                            .build();
                }
            }
            case ROLLBACK:
                return new EdmlRequestContext(
                        createRequestMetrics(request),
                        new DatamartRequest(request),
                        node,
                        envName);
            default:
                return DmlRequestContext.builder()
                        .request(new DmlRequest(request))
                        .envName(envName)
                        .metrics(createRequestMetrics(request))
                        .sourceType(getDmlSourceType(node))
                        .sqlNode(node)
                        .build();
        }
    }

    private SourceType getDmlSourceType(SqlNode node) {
        if (node instanceof SqlDataSourceTypeGetter) {
            SqlCharStringLiteral dsTypeNode = ((SqlDataSourceTypeGetter) node).getDatasourceType();
            if (dsTypeNode != null) {
                return SourceType.valueOfAvailable(dsTypeNode.getNlsString().getValue());
            }
        }
        return null;
    }

    private RequestMetrics createRequestMetrics(QueryRequest request) {
        return RequestMetrics.builder()
                .startTime(LocalDateTime.now(CoreConstants.CORE_ZONE_ID))
                .requestId(request.getRequestId())
                .status(RequestStatus.IN_PROCESS)
                .isActive(true)
                .build();
    }

    private boolean isConfigRequest(SqlNode node) {
        return node instanceof SqlConfigCall;
    }

    private boolean isDdlRequest(SqlNode node) {
        return node instanceof SqlDdl || node instanceof SqlAlter || node instanceof SqlBaseTruncate;
    }

}
