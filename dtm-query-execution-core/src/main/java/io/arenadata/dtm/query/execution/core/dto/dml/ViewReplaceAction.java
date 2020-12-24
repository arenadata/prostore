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
package io.arenadata.dtm.query.execution.core.dto.dml;

import lombok.Data;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSnapshot;

import java.util.function.Consumer;

@Data
public class ViewReplaceAction {
    private final Consumer<SqlNode> consumer;
    private final boolean needWrap;
    private final SqlNode from;
    private DatamartViewPair viewPair;
    private SqlNode to;

    public ViewReplaceAction(SqlNode from, Consumer<SqlNode> consumer) {
        this(from, false, consumer);
    }

    public ViewReplaceAction(SqlNode from, boolean needWrap, Consumer<SqlNode> consumer) {
        this.needWrap = needWrap;
        this.consumer = consumer;
        this.from = from;
        this.viewPair = new DatamartViewPair(getDatamart(from), getViewName(from));
    }

    private String getViewName(SqlNode from) {
        if (from instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) from;
            String name = identifier.names.size() > 1 ? identifier.names.get(1) : identifier.names.get(0);
            return name.toLowerCase();
        } else if (from instanceof SqlSnapshot) {
            return getViewName(((SqlSnapshot) from).getTableRef());
        } else throw new IllegalArgumentException("Node required instance of SqlIdentifier or SqlSnapshot");
    }

    private String getDatamart(SqlNode from) {
        if (from instanceof SqlIdentifier) {
            SqlIdentifier identifier = (SqlIdentifier) from;
            return identifier.names.size() > 1 ? identifier.names.get(0).toLowerCase() : null;
        } else if (from instanceof SqlSnapshot) {
            return getDatamart(((SqlSnapshot) from).getTableRef());
        } else throw new IllegalArgumentException("Node required instance of SqlIdentifier or SqlSnapshot");
    }

    public void run() {
        consumer.accept(to);
    }
}

