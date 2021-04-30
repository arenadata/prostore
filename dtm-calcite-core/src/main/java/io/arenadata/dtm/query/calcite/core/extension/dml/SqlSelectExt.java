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
package io.arenadata.dtm.query.calcite.core.extension.dml;

import lombok.Getter;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

@Getter
public class SqlSelectExt extends SqlSelect implements SqlDataSourceTypeGetter {

    private SqlCharStringLiteral datasourceType;

    public SqlSelectExt(SqlParserPos pos,
                        SqlNodeList keywordList,
                        SqlNodeList selectList,
                        SqlNode from,
                        SqlNode where,
                        SqlNodeList groupBy,
                        SqlNode having,
                        SqlNodeList windowDecls,
                        SqlNodeList orderBy,
                        SqlNode offset,
                        SqlNode fetch,
                        SqlNodeList hints,
                        SqlNode datasourceType) {
        super(pos, keywordList, selectList, from, where, groupBy, having, windowDecls, orderBy, offset, fetch, hints);
        this.datasourceType = (SqlCharStringLiteral) datasourceType;
    }

}
