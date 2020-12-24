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
package io.arenadata.dtm.query.execution.core.utils;

import io.arenadata.dtm.common.reader.InformationSchemaView;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MetaDataQueryPreparerTest {

  @Test
  void findInformationSchemaViewsSingle() {
    String sql = "select * from INFORMATION_SCHEMA.TABLE_CONSTRAINTS";

    assertEquals(InformationSchemaView.TABLE_CONSTRAINTS,
      MetaDataQueryPreparer.findInformationSchemaViews(sql).get(0).getView());
  }

  @Test
  void findInformationSchemaViewsSingleWithQuotesInTable() {
    String sql = "select * from INFORMATION_SCHEMA.\"TABLE_CONSTRAINTS\"";

    assertEquals(InformationSchemaView.TABLE_CONSTRAINTS,
      MetaDataQueryPreparer.findInformationSchemaViews(sql).get(0).getView());
  }

  @Test
  void findInformationSchemaViewsSingleWithQuotesInSchema() {
    String sql = "select * from \"INFORMATION_SCHEMA\".TABLE_CONSTRAINTS";

    assertEquals(InformationSchemaView.TABLE_CONSTRAINTS,
      MetaDataQueryPreparer.findInformationSchemaViews(sql).get(0).getView());
  }

  @Test
  void findInformationSchemaViewsSingleWithQuotesInTableAndSchema() {
    String sql = "select * from \"INFORMATION_SCHEMA\".\"TABLE_CONSTRAINTS\"";

    assertEquals(InformationSchemaView.TABLE_CONSTRAINTS,
      MetaDataQueryPreparer.findInformationSchemaViews(sql).get(0).getView());
  }

  @Test
  void findInformationSchemaViewsSingleCaseInsensitive() {
    String sql = "select * from infORMATION_SCheMA.SCheMaTa";

    assertEquals(InformationSchemaView.SCHEMATA,
      MetaDataQueryPreparer.findInformationSchemaViews(sql).get(0).getView());
  }

  @Test
  void findInformationSchemaViewsMulti() {
    String sql = "select (select max(deltas.load_id) from information_schema.\"deltas\" deltas) as load_id, tables.*" +
      " from INFORMATION_SCHEMA.TABLES tables" +
      " where exists(select 1 from \"INFORMATION_SCHEMA\".schemata schemata where schemata.schema_name = tables.table_schema)";

    assertEquals(3,  MetaDataQueryPreparer.findInformationSchemaViews(sql).size());
  }

  @Test
  void modifySingle() {
    String sql = "select * from infORMATION_SCheMA.SCheMaTa";
    String expectedSql = "select * from logic_schema_datamarts";
    assertEquals(expectedSql,
      MetaDataQueryPreparer.modify(sql).toLowerCase());
  }

  @Test
  void modifyMulti() {
    String sql = "select (select max(deltas.load_id) from information_schema.\"deltas\" deltas) as load_id, tables.*" +
      " from INFORMATION_SCHEMA.TABLES tables" +
      " where exists(select 1 from \"INFORMATION_SCHEMA\".schemata schemata where schemata.schema_name = tables.table_schema)";
    String expectedSql = "select (select max(deltas.load_id) from logic_schema_deltas deltas) as load_id, tables.*" +
      " from logic_schema_entities tables" +
      " where exists(select 1 from logic_schema_datamarts schemata where schemata.schema_name = tables.table_schema)";
    assertEquals(expectedSql,
      MetaDataQueryPreparer.modify(sql).toLowerCase());
  }
}
