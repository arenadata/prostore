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

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;

import java.util.*;

@Data
public class ReplaceContext {
    private final Map<DatamartViewPair, DatamartViewWrap> viewMap;
    private final Handler<AsyncResult<String>> resultHandler;
    private final List<ViewReplaceAction> resultActions;
    private final List<ViewReplaceAction> tempActions;
    private final Set<DatamartViewPair> tables;
    private final SqlNode rootSqlNode;
    private final String defaultDatamart;

    public ReplaceContext(SqlNode rootSqlNode,
                          String defaultDatamart,
                          Handler<AsyncResult<String>> resultHandler) {
        this.rootSqlNode = rootSqlNode;
        this.defaultDatamart = defaultDatamart;
        this.resultHandler = resultHandler;
        resultActions = new ArrayList<>();
        tempActions = new ArrayList<>();
        viewMap = new HashMap<>();
        tables = new HashSet<>();
    }
}
