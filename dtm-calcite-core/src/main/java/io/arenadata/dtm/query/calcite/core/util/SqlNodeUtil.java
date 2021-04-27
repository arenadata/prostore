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
package io.arenadata.dtm.query.calcite.core.util;

import io.arenadata.dtm.query.calcite.core.extension.dml.LimitableSqlOrderBy;
import io.arenadata.dtm.query.calcite.core.extension.parser.ParseException;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;

public final class SqlNodeUtil {
    private SqlNodeUtil() {
    }

    public static SqlNode copy(SqlNode sqlNode) {
        return new SqlSelectTree(sqlNode).copy().getRoot().getNode();
    }

    public static SqlNode checkViewQueryAndGet(SqlNode query) throws ParseException {
        if (query instanceof SqlSelect) {
            if (((SqlSelect) query).getFrom() == null) {
                throw new ParseException("View query must have from clause!");
            } else {
                return query;
            }
        } else if (query instanceof LimitableSqlOrderBy) {
            checkViewQueryAndGet(((LimitableSqlOrderBy) query).query);
            return query;
        } else {
            throw new ParseException(String.format("Type %s of query does not support!", query.getClass().getName()));
        }
    }
}
