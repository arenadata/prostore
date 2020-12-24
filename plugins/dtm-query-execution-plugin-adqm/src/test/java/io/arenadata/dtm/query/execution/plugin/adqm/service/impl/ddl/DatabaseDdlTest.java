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
package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.ddl;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.calcite.core.extension.eddl.SqlCreateDatabase;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.AppConfiguration;
import io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties.DdlProperties;
import io.arenadata.dtm.query.execution.plugin.adqm.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.service.mock.MockDatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adqm.service.mock.MockEnvironment;
import io.arenadata.dtm.query.execution.plugin.api.ddl.DdlRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DdlRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.ddl.DdlExecutor;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertTrue;

class DatabaseDdlTest {
    private static final DdlProperties ddlProperties = new DdlProperties();
    private static final AppConfiguration appConfiguration = new AppConfiguration(new MockEnvironment());

    @BeforeAll
    public static void setup() {
        ddlProperties.setCluster("test_cluster");
    }

    @Test
    public void testCreateDatabase() {
        SqlParserPos pos = new SqlParserPos(1, 1);
        SqlCreateDatabase createDatabase = new SqlCreateDatabase(pos, true,
                new SqlIdentifier("testdb", pos));
        DdlRequestContext context = new DdlRequestContext(null, createDatabase);

        DatabaseExecutor executor = new MockDatabaseExecutor(
                Collections.singletonList(t -> t.equalsIgnoreCase("CREATE DATABASE IF NOT EXISTS dev__testdb on cluster test_cluster")));

        DdlExecutor<Void> databaseDdlService = new CreateDatabaseExecutor(executor, ddlProperties, appConfiguration);

        databaseDdlService.execute(context, "CREATE", ar -> assertTrue(ar.succeeded()));
    }

    @Test
    public void testDropDatabase() {
        final String datamartName = "testdb";
        DdlRequestContext context = new DdlRequestContext(new DdlRequest(new QueryRequest()));
        context.setDatamartName(datamartName);

        DatabaseExecutor executor = new MockDatabaseExecutor(
                Collections.singletonList(t -> t.equalsIgnoreCase("drop database if exists dev__testdb on cluster test_cluster")));

        DdlExecutor<Void> databaseDdlService = new DropDatabaseExecutor(executor, ddlProperties, appConfiguration);

        databaseDdlService.execute(context, "DROP", ar -> assertTrue(ar.succeeded()));
    }
}
