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
package io.arenadata.dtm.query.calcite.core.extension.eddl;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

public class DropDatabase extends SqlDrop {
  private final SqlIdentifier name;

  private static final SqlOperator OPERATOR_DATABASE =
    new SqlSpecialOperator("DROP DATABASE", SqlKind.DROP_SCHEMA);

  public DropDatabase(SqlParserPos pos, boolean ifExists, SqlIdentifier name) {
    super(OPERATOR_DATABASE, pos, ifExists);
    this.name = name;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableList.of(name);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword(this.getOperator().getName());
    if (ifExists) {
      writer.keyword("IF EXISTS");
    }
    name.unparse(writer, leftPrec, rightPrec);
  }

  public SqlIdentifier getName() {
    return name;
  }
}
