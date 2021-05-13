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
package io.arenadata.dtm.query.execution.core.dml.service.impl;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.query.calcite.core.extension.snapshot.SqlDeltaSnapshot;
import io.arenadata.dtm.query.calcite.core.node.SqlSelectTree;
import io.arenadata.dtm.query.calcite.core.node.SqlTreeNode;
import io.arenadata.dtm.query.calcite.core.service.DefinitionService;
import io.arenadata.dtm.query.calcite.core.util.SqlNodeUtil;
import io.arenadata.dtm.query.execution.core.base.repository.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.dml.service.LogicViewReplacer;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class LogicViewReplacerImpl implements LogicViewReplacer {
    private static final SqlDialect SQL_DIALECT = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
    private final DefinitionService<SqlNode> definitionService;
    private final EntityDao entityDao;

    @Autowired
    public LogicViewReplacerImpl(
            @Qualifier("coreCalciteDefinitionService") DefinitionService<SqlNode> definitionService,
            EntityDao entityDao) {
        this.definitionService = definitionService;
        this.entityDao = entityDao;
    }

    @SneakyThrows
    @Override
    @Deprecated
    public Future<String> replace(String sql, String datamart) {
        return Future.future((Promise<String> promise) -> {
            log.debug("before replacing:\n{}", sql);
            SqlNode rootSqlNode = definitionService.processingQuery(sql);
            replace(rootSqlNode,
                    datamart,
                    null,
                    null,
                    null,
                    null)
                    .onSuccess(v -> {
                        String replacedSql = rootSqlNode.toSqlString(SQL_DIALECT).getSql();
                        log.debug("after replacing:\n{}", replacedSql);
                        promise.complete(replacedSql);
                    })
                    .onFailure(promise::fail);
        });
    }

    @SneakyThrows
    @Override
    public Future<SqlNode> replace(SqlNode sql, String datamart) {
        return Future.future((Promise<SqlNode> promise) -> {
            log.debug("before replacing:\n{}", sql);
            SqlNode rootSqlNode = SqlNodeUtil.copy(sql);
            replace(rootSqlNode,
                datamart,
                null,
                null,
                null,
                null)
                .onSuccess(v -> {
                    log.debug("after replacing: [{}]", rootSqlNode);
                    promise.complete(rootSqlNode);
                })
                .onFailure(promise::fail);
        });
    }

    private Future<Void> replace(SqlNode viewQueryNode,
                                 String datamart,
                                 SqlSelectTree parentTree,
                                 SqlTreeNode parentNode,
                                 SqlSnapshot sqlSnapshot,
                                 Entity view) {
        SqlSelectTree tree = new SqlSelectTree(viewQueryNode);
        List<SqlTreeNode> allTableAndSnapshots = tree.findAllTableAndSnapshots();
        return CompositeFuture.join(allTableAndSnapshots.stream()
                .map(childNode -> entityDao.getEntity(
                        childNode.tryGetSchemaName().orElse(datamart),
                        childNode.tryGetTableName().orElseThrow(() -> new RuntimeException("Can't get tableName"))
                        ).compose(entity -> {
                            SqlSnapshot currSnapshot = getSqlSnapshot(parentNode, sqlSnapshot);
                            if (entity.getEntityType() == EntityType.VIEW) {
                                return replace(definitionService.processingQuery(entity.getViewQuery()),
                                        datamart,
                                        tree,
                                        childNode,
                                        currSnapshot,
                                        entity);
                            } else {
                                if (currSnapshot != null) {
                                    SqlDeltaSnapshot parentSnapshot = parentNode.getNode();
                                    SqlSnapshot childSnapshot = parentSnapshot.copy(childNode.getNode());
                                    childNode.getSqlNodeSetter().accept(childSnapshot);
                                }
                                return Future.succeededFuture();
                            }
                        })
                )
                .collect(Collectors.toList()))
                .onSuccess(cf -> {
                    if (parentNode != null && view != null) {
                        if (isAliasExists(parentTree, parentNode)) {
                            parentNode.getSqlNodeSetter().accept(tree.getRoot().getNode());
                        } else {
                            SqlBasicCall alias = getAlias(parentNode, tree);
                            parentNode.getSqlNodeSetter().accept(alias);
                        }
                    }
                })
                .mapEmpty();
    }

    private SqlSnapshot getSqlSnapshot(SqlTreeNode parentNode, SqlSnapshot sqlSnapshot) {
        if (parentNode == null) {
            return null;
        } else {
            return sqlSnapshot == null ?
                    (parentNode.getNode() instanceof SqlSnapshot ? parentNode.getNode() : null)
                    : sqlSnapshot;
        }
    }

    private SqlBasicCall getAlias(SqlTreeNode parentNode, SqlSelectTree tree) {
        String tableName = parentNode.tryGetTableName()
                .orElseThrow(() -> new RuntimeException("Can't get tableName"));
        return new SqlBasicCall(new SqlAsOperator(),
                new SqlNode[]{tree.getRoot().getNode(),
                        new SqlIdentifier(tableName, SqlParserPos.QUOTED_ZERO)},
                SqlParserPos.QUOTED_ZERO);
    }

    private boolean isAliasExists(SqlSelectTree tree, SqlTreeNode node) {
        return tree.getParentByChild(node)
                .filter(parentNode -> parentNode.getKindPath().endsWith(".AS"))
                .isPresent();
    }

}


