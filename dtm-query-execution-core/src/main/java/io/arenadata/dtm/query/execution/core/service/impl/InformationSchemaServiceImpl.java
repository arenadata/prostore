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
package io.arenadata.dtm.query.execution.core.service.impl;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.model.ddl.EntityField;
import io.arenadata.dtm.common.model.ddl.EntityType;
import io.arenadata.dtm.common.reader.InformationSchemaView;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlCreateTable;
import io.arenadata.dtm.query.calcite.core.extension.ddl.SqlCreateView;
import io.arenadata.dtm.query.execution.core.dao.exception.datamart.DatamartAlreadyExistsException;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.DatamartDao;
import io.arenadata.dtm.query.execution.core.dao.servicedb.zookeeper.EntityDao;
import io.arenadata.dtm.query.execution.core.service.DdlQueryGenerator;
import io.arenadata.dtm.query.execution.core.service.InformationSchemaService;
import io.arenadata.dtm.query.execution.core.service.hsql.HSQLClient;
import io.arenadata.dtm.query.execution.core.utils.InformationSchemaUtils;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import lombok.var;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Component
public class InformationSchemaServiceImpl implements InformationSchemaService {

    private static final int TABLE_NAME_COLUMN_INDEX = 0;
    private static final int ORDINAL_POSITION_COLUMN_INDEX = 1;
    private static final int COLUMN_NAME_COLUMN_INDEX = 2;
    private static final int DATA_TYPE_COLUMN_INDEX = 3;
    private static final int IS_NULLABLE_COLUMN_INDEX = 4;
    private static final String IS_NULLABLE_COLUMN_TRUE = "YES";

    private final ApplicationContext applicationContext;
    private final DdlQueryGenerator ddlQueryGenerator;
    private final DatamartDao datamartDao;
    private final EntityDao entityDao;
    private final HSQLClient client;

    @Autowired
    public InformationSchemaServiceImpl(HSQLClient client,
                                        DatamartDao datamartDao,
                                        EntityDao entityDao,
                                        DdlQueryGenerator ddlQueryGenerator,
                                        ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        this.ddlQueryGenerator = ddlQueryGenerator;
        this.datamartDao = datamartDao;
        this.entityDao = entityDao;
        this.client = client;
    }

    @Override
    public Future<Void> update(SqlNode sql) {
        switch (sql.getKind()) {
            case CREATE_TABLE:
                return createTable((SqlCreateTable) sql);
            case CREATE_SCHEMA:
            case DROP_SCHEMA:
                return createOrDropSchema(sql);
            case CREATE_VIEW:
                return createOrReplaceView((SqlCreateView) sql);
            case ALTER_VIEW:
            case DROP_VIEW:
            case DROP_TABLE:
                return client.executeQuery(sql.toString().replace("`", ""))
                    .onFailure(this::shutdown);
            default:
                throw new IllegalArgumentException("Sql type not supported: " + sql.getKind());
        }
    }

    private void shutdown(Throwable throwable) {
        val exitCode = SpringApplication.exit(applicationContext, () -> 1);
        System.exit(exitCode);
    }

    private Future<Void> createOrReplaceView(SqlCreateView sqlCreateView) {
        if (sqlCreateView.getReplace()) {
            return client.executeQuery(String.format(InformationSchemaUtils.DROP_VIEW, sqlCreateView.getName()))
                .compose(v -> client.executeQuery(String.format(InformationSchemaUtils.CREATE_VIEW,
                    sqlCreateView.getName(),
                    sqlCreateView.getQuery().toString().replace("`", ""))));
        } else {
            return client.executeQuery(sqlCreateView.toString().replace("`", ""))
                .onFailure(this::shutdown);
        }
    }

    private Future<Void> createOrDropSchema(SqlNode sql) {
        var schemaSql = sql.toString()
            .replace("DATABASE", "SCHEMA")
            .replace("`", "");
        schemaSql = SqlKind.DROP_SCHEMA == sql.getKind() ? schemaSql + " CASCADE" : schemaSql;
        return client.executeQuery(schemaSql)
            .onFailure(this::shutdown);
    }

    private Future<Void> createTable(SqlCreateTable createTable) {
        val distributedByColumns = createTable.getDistributedBy().getDistributedBy().getList().stream()
            .map(SqlNode::toString)
            .collect(Collectors.toList());
        val schemaTable = createTable.getOperandList().get(0).toString();
        val table = getTableName(schemaTable);
        val creatTableQuery = sqlWithoutDistributedBy(createTable);

        List<String> commentQueries = new ArrayList<>();
        val columns = ((SqlNodeList) createTable.getOperandList().get(1)).getList();
        columns.stream()
            .filter(node -> node instanceof SqlColumnDeclaration)
            .map(node -> (SqlColumnDeclaration) node)
            .forEach(column -> {
                val columnName = column.getOperandList().get(0).toString();
                val typeString = getTypeWithoutSize(column.getOperandList().get(1).toString());
                val type = ColumnType.fromTypeString(typeString);
                if (needComment(type)) {
                    commentQueries.add(commentOnColumn(schemaTable, columnName, typeString));
                }
            });

        return client.executeQuery(creatTableQuery)
            .compose(r -> client.executeQuery(createShardingKeyIndex(table, schemaTable, distributedByColumns)))
            .compose(r -> client.executeBatch(commentQueries))
            .onFailure(this::shutdown);
    }

    private String getTypeWithoutSize(String type) {
        val idx = type.indexOf("(");
        if (idx != -1) {
            return type.substring(0, idx);
        }
        return type;
    }

    private String sqlWithoutDistributedBy(SqlCreateTable createTable) {
        String sqlString = createTable.toString();
        return sqlString.substring(0, sqlString.indexOf("DISTRIBUTED BY") - 1).replace("`", "");
    }

    private String commentOnColumn(String schemaTable, String column, String comment) {
        return String.format(InformationSchemaUtils.COMMENT_ON_COLUMN, schemaTable, column, comment);
    }

    private String createShardingKeyIndex(String table, String schemaTable, List<String> columns) {
        return String.format(InformationSchemaUtils.CREATE_SHARDING_KEY_INDEX,
            table, schemaTable, String.join(", ", columns));
    }

    private String getTableName(String schemaTable) {
        return schemaTable.substring(schemaTable.indexOf(".") + 1);
    }

    @Override
    public Future<Void> createInformationSchemaViews() {
        log.info("Information schema initialized start");
        return client.executeBatch(informationSchemaViewsQueries())
            .compose(v -> createSchemasFromDatasource())
            .compose(v -> initEntities())
            .onSuccess(v -> log.info("Information schema initialized successfully"))
            .onFailure(err -> log.error("Error while creating information schema views", err));
    }

    private Future<Void> initEntities() {
        return Future.future(promise -> client.getQueryResult(createInitEntitiesQuery())
            .onSuccess(resultSet -> {
                try {
                    Map<String, List<EntityField>> fieldsByView = resultSet.getResults().stream()
                        .collect(Collectors.groupingBy(col -> col.getString(TABLE_NAME_COLUMN_INDEX),
                            Collectors.mapping(this::createField, Collectors.toList())));

                    val entities = Arrays.stream(InformationSchemaView.values())
                        .flatMap(view -> {
                            final String viewRealName = view.getRealName().toUpperCase();
                            return Optional.ofNullable(fieldsByView.get(viewRealName))
                                .map(fields -> createEntity(view, fields).stream())
                                .orElseThrow(() -> new RuntimeException(
                                    String.format("View [%s.%s] doesn't exist",
                                        InformationSchemaView.DTM_SCHEMA_NAME, viewRealName)));
                        })
                        .collect(Collectors.toList());

                    createLogicSchemaDatamartInDatasource()
                        .compose(r -> storeLogicSchemaInDatasource(entities))
                        .onSuccess(success -> promise.complete())
                        .onFailure(promise::fail);

                } catch (Exception e) {
                    promise.fail(e);
                }
            })
            .onFailure(promise::fail));
    }

    private Future<Void> createLogicSchemaDatamartInDatasource() {
        return Future.future(promise ->
            createSchemaIfNotExists()
                .onSuccess(r -> promise.complete())
                .onFailure(error -> {
                    if (error instanceof DatamartAlreadyExistsException) {
                        promise.complete();
                    } else {
                        promise.fail(error);
                    }
                }));
    }

    private Future<Void> createSchemaIfNotExists() {
        String schemaName = InformationSchemaView.SCHEMA_NAME.toLowerCase();
        return datamartDao.existsDatamart(schemaName)
            .compose(isExists -> isExists ? Future.succeededFuture() : datamartDao.createDatamart(schemaName));
    }

    private Future<Void> storeLogicSchemaInDatasource(List<Entity> entities) {
        return Future.future(p ->
            CompositeFuture.join(entities.stream()
                .map(this::createEntityIfNotExists)
                .collect(Collectors.toList()))
                .onSuccess(success -> p.complete())
                .onFailure(p::fail));
    }

    private Future<Void> createEntityIfNotExists(Entity entity) {
        return entityDao.existsEntity(entity.getSchema(), entity.getName())
            .compose(isExists -> isExists ? Future.succeededFuture() : entityDao.createEntity(entity));
    }

    private String createInitEntitiesQuery() {
        return String.format("SELECT TABLE_NAME, ORDINAL_POSITION, COLUMN_NAME," +
                "  case" +
                "    when DATA_TYPE = 'DOUBLE PRECISION' then 'DOUBLE'" +
                "    when DATA_TYPE = 'CHARACTER VARYING' then 'VARCHAR'" +
                "    when DATA_TYPE = 'INTEGER' then 'INT'" +
                "    when DATA_TYPE = 'CHARACTER' then 'CHAR'" +
                "    else DATA_TYPE end as DATA_TYPE," +
                " IS_NULLABLE" +
                " FROM information_schema.columns WHERE TABLE_SCHEMA = '%s' and TABLE_NAME in (%s);",
            InformationSchemaView.DTM_SCHEMA_NAME,
            Arrays.stream(InformationSchemaView.values())
                .map(view -> String.format("'%s'", view.getRealName().toUpperCase()))
                .collect(Collectors.joining(",")));
    }

    private EntityField createField(final JsonArray jsonArray) {
        return EntityField.builder()
            .ordinalPosition(jsonArray.getInteger(ORDINAL_POSITION_COLUMN_INDEX))
            .name(jsonArray.getString(COLUMN_NAME_COLUMN_INDEX).toLowerCase())
            .type(ColumnType.valueOf(jsonArray.getString(DATA_TYPE_COLUMN_INDEX)))
            .nullable(IS_NULLABLE_COLUMN_TRUE.equals(jsonArray.getString(IS_NULLABLE_COLUMN_INDEX)))
            .build();
    }

    private List<Entity> createEntity(final InformationSchemaView view, final List<EntityField> fields) {
        return Arrays.asList(
            Entity.builder()
                .schema(InformationSchemaView.SCHEMA_NAME.toLowerCase())
                .entityType(EntityType.VIEW)
                .name(view.name().toLowerCase())
                .viewQuery(String.format("select * from %s.%s", InformationSchemaView.SCHEMA_NAME.toLowerCase(),
                    view.getRealName()))
                .fields(fields)
                .build(),
            Entity.builder()
                .schema(InformationSchemaView.SCHEMA_NAME.toLowerCase())
                .entityType(EntityType.TABLE)
                .name(view.getRealName().toLowerCase())
                .fields(fields)
                .build());
    }

    private List<String> informationSchemaViewsQueries() {
        return Arrays.asList(
            String.format(InformationSchemaUtils.CREATE_SCHEMA, InformationSchemaView.DTM_SCHEMA_NAME),
            InformationSchemaUtils.LOGIC_SCHEMA_DATAMARTS,
            InformationSchemaUtils.LOGIC_SCHEMA_ENTITIES,
            InformationSchemaUtils.LOGIC_SCHEMA_COLUMNS,
            InformationSchemaUtils.LOGIC_SCHEMA_KEY_COLUMN_USAGE,
            InformationSchemaUtils.LOGIC_SCHEMA_ENTITY_CONSTRAINTS);
    }

    private Future<Void> createSchemasFromDatasource() {
        return datamartDao.getDatamarts()
            .compose(this::createSchemas);
    }

    private Future<Void> createSchemas(List<String> datamarts) {
        return Future.future(p -> CompositeFuture.join(datamarts.stream()
            .filter(datamart -> !InformationSchemaView.SCHEMA_NAME.equals(datamart.toUpperCase()))
            .map(this::createSchemaForDatamart)
            .collect(Collectors.toList()))
            .onSuccess(success -> p.complete())
            .onFailure(p::fail));
    }

    private Future<Void> createSchemaForDatamart(String datamart) {
        val query = String.format(InformationSchemaUtils.CREATE_SCHEMA, datamart);
        return client.executeQuery(query)
            .compose(r -> entityDao.getEntityNamesByDatamart(datamart))
            .compose(entityNames -> getEntitiesByNames(datamart, entityNames))
            .compose(entities -> client.executeBatch(getEntitiesCreateQueries(entities)));
    }

    private Future<List<Entity>> getEntitiesByNames(String datamart, List<String> entitiesNames) {
        return Future.future(promise -> {
            CompositeFuture.join(entitiesNames.stream()
                .map(entity -> entityDao.getEntity(datamart, entity))
                .collect(Collectors.toList()))
                .onSuccess(entityResult -> promise.complete(entityResult.list()))
                .onFailure(promise::fail);
        });
    }

    private List<String> getEntitiesCreateQueries(List<Entity> entities) {
        List<String> viewEntities = new ArrayList<>();
        List<String> tableEntities = new ArrayList<>();
        List<String> commentQueries = new ArrayList<>();
        List<String> createShardingKeys = new ArrayList<>();
        entities.forEach(entity -> {
            if (EntityType.VIEW.equals(entity.getEntityType())) {
                viewEntities.add(ddlQueryGenerator.generateCreateViewQuery(entity));
            }
            if (EntityType.TABLE.equals(entity.getEntityType())) {
                tableEntities.add(ddlQueryGenerator.generateCreateTableQuery(entity));
                entity.getFields()
                    .forEach(field -> {
                        val type = field.getType();
                        if (needComment(type)) {
                            commentQueries.add(commentOnColumn(entity.getNameWithSchema(), field.getName(), type.toString()));
                        }
                    });
                val shardingKeyColumns = entity.getFields().stream()
                    .filter(field -> field.getShardingOrder() != null)
                    .map(EntityField::getName)
                    .collect(Collectors.toList());
                createShardingKeys.add(createShardingKeyIndex(entity.getName(), entity.getNameWithSchema(), shardingKeyColumns));
            }
        });
        return Stream.of(tableEntities, viewEntities, commentQueries, createShardingKeys)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    }

    private boolean needComment(ColumnType type) {
        switch (type) {
            case DOUBLE:
            case FLOAT:
            case INT:
            case VARCHAR:
                return true;
            default:
                return false;
        }
    }
}
