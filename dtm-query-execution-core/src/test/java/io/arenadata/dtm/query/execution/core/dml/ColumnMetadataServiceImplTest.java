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
package io.arenadata.dtm.query.execution.core.dml;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.arenadata.dtm.common.dto.QueryParserRequest;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.query.calcite.core.service.QueryParserService;
import io.arenadata.dtm.query.execution.core.calcite.configuration.CalciteConfiguration;
import io.arenadata.dtm.query.execution.core.calcite.factory.CoreCalciteSchemaFactory;
import io.arenadata.dtm.query.execution.core.calcite.factory.CoreSchemaFactory;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteContextProvider;
import io.arenadata.dtm.query.execution.core.calcite.service.CoreCalciteDMLQueryParserService;
import io.arenadata.dtm.query.execution.core.dml.service.ColumnMetadataService;
import io.arenadata.dtm.query.execution.core.dml.service.impl.ColumnMetadataServiceImpl;
import io.arenadata.dtm.query.execution.core.utils.TestUtils;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.vertx.core.Vertx;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.List;

import static io.arenadata.dtm.query.execution.core.utils.TestUtils.loadTextFromFile;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@ExtendWith(VertxExtension.class)
class ColumnMetadataServiceImplTest {
    private final CalciteConfiguration calciteConfiguration = new CalciteConfiguration();
    private final SqlParser.Config configParser = calciteConfiguration.configEddlParser(calciteConfiguration.getSqlParserFactory());
    private final CoreCalciteSchemaFactory calciteSchemaFactory = new CoreCalciteSchemaFactory(new CoreSchemaFactory());
    private final CoreCalciteContextProvider calciteContextProvider = new CoreCalciteContextProvider(configParser, calciteSchemaFactory);
    private final QueryParserService parserService = new CoreCalciteDMLQueryParserService(calciteContextProvider, Vertx.vertx());
    private final ColumnMetadataService service = new ColumnMetadataServiceImpl(parserService);

    @Test
    void getColumnMetadata(VertxTestContext testContext) throws JsonProcessingException {
        val sql = "select * from dml.accounts";
        val datamarts = DatabindCodec.mapper()
                .readValue(loadTextFromFile("schema/dml_all_types.json"), new TypeReference<List<Datamart>>() {
                });
        List<ColumnMetadata> expectedColumns = Arrays.asList(
                new ColumnMetadata("id", ColumnType.INT),
                new ColumnMetadata("double_col", ColumnType.DOUBLE),
                new ColumnMetadata("float_col", ColumnType.FLOAT),
                new ColumnMetadata("varchar_col", ColumnType.VARCHAR, 36),
                new ColumnMetadata("boolean_col", ColumnType.BOOLEAN),
                new ColumnMetadata("int_col", ColumnType.INT),
                new ColumnMetadata("bigint_col", ColumnType.BIGINT),
                new ColumnMetadata("date_col", ColumnType.DATE),
                new ColumnMetadata("timestamp_col", ColumnType.TIMESTAMP, 6),
                new ColumnMetadata("time_col", ColumnType.TIME, 5),
                new ColumnMetadata("uuid_col", ColumnType.UUID, 36),
                new ColumnMetadata("char_col", ColumnType.CHAR, 10),
                new ColumnMetadata("int32_col", ColumnType.INT32),
                new ColumnMetadata("link_col", ColumnType.LINK));
        SqlNode sqlNode = TestUtils.DEFINITION_SERVICE.processingQuery(sql);
        service.getColumnMetadata(new QueryParserRequest(sqlNode, datamarts))
                .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
                    assertEquals(expectedColumns, result);
                    testContext.completeNow();
                })));
    }
}
