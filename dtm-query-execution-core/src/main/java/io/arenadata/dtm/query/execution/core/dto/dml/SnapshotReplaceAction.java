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
public class SnapshotReplaceAction {
    private final Consumer<SqlNode> consumer;
    private final SqlIdentifier from;
    private String viewName;
    private SqlSnapshot to;

    public SnapshotReplaceAction(SqlIdentifier from, Consumer<SqlNode> consumer) {
        this.consumer = consumer;
        this.from = from;
    }

    public void run() {
        consumer.accept(to);
    }
}
