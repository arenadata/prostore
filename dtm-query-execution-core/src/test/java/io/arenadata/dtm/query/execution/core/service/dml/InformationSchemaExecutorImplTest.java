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
package io.arenadata.dtm.query.execution.core.service.dml;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.common.reader.QuerySourceRequest;
import io.arenadata.dtm.common.reader.SourceType;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.service.dml.impl.InformationSchemaExecutorImpl;
import io.arenadata.dtm.query.execution.core.service.hsql.HSQLClient;
import io.arenadata.dtm.query.execution.core.service.hsql.impl.HSQLClientImpl;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import org.apache.calcite.sql.SqlDialect;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;


public class InformationSchemaExecutorImplTest {

    private HSQLClient client = mock(HSQLClientImpl.class);
    private InformationSchemaExecutor informationSchemaExecutor = new InformationSchemaExecutorImpl(client,
        new SqlDialect(SqlDialect.EMPTY_CONTEXT),
        mock(QueryParserService.class));

    @Test
    @Disabled
    void executeQuery() {
        //FIXME
        List<ColumnMetadata> metadata = new ArrayList<>();
        metadata.add(new ColumnMetadata("schema_name", ColumnType.VARCHAR));
        QuerySourceRequest sourceRequest = new QuerySourceRequest();
        final Map<String, Object> rowMap = new HashMap<>();
        rowMap.put("schema_name", "test_datamart");
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setRequestId(UUID.randomUUID());
        queryRequest.setSql("select * from \"INFORMATION_SCHEMA\".schemata");

        sourceRequest.setQueryRequest(queryRequest);
        sourceRequest.setMetadata(metadata);
        sourceRequest.setSourceType(SourceType.INFORMATION_SCHEMA);

        informationSchemaExecutor.execute(sourceRequest, ar -> {
            assertTrue(ar.succeeded());
            Map<String, Object> expectedMap = new HashMap<>();
            expectedMap.put("schema_name", "test_datamart");
            assertEquals(expectedMap, ar.result().getResult().get(0));
        });
    }
}
