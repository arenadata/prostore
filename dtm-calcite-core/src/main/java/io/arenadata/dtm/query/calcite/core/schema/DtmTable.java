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
package io.arenadata.dtm.query.calcite.core.schema;

import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.calcite.core.util.CalciteUtil;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;

import java.util.ArrayList;

public abstract class DtmTable extends AbstractQueryableTable implements TranslatableTable {

    protected static final int UUID_SIZE = 36;
    protected final QueryableSchema dtmSchema;
    protected final Entity entity;

    public DtmTable(QueryableSchema dtmSchema, Entity entity) {
        super(Object[].class);
        this.dtmSchema = dtmSchema;
        this.entity = entity;
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        //TODO: complete the task of executing the request
        return null;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
        entity.getFields().forEach(it -> {
                    if (it.getSize() != null && it.getAccuracy() != null) {
                        builder.add(it.getName(), CalciteUtil.valueOf(it.getType()), it.getSize(), it.getAccuracy())
                                .nullable(it.getNullable() != null && it.getNullable());
                    } else if (it.getSize() != null) {
                        builder.add(it.getName(), CalciteUtil.valueOf(it.getType()), it.getSize())
                                .nullable(it.getNullable() != null && it.getNullable());
                    } else {
                        if (it.getType() == ColumnType.UUID) {
                            builder.add(it.getName(), CalciteUtil.valueOf(it.getType()), UUID_SIZE)
                                    .nullable(it.getNullable() != null && it.getNullable());
                        } else if ((it.getType() == ColumnType.TIME || it.getType() == ColumnType.TIMESTAMP)
                                && it.getAccuracy() != null) {
                            builder.add(it.getName(), CalciteUtil.valueOf(it.getType()), it.getAccuracy())
                                    .nullable(it.getNullable() != null && it.getNullable());
                        } else {
                            builder.add(it.getName(), CalciteUtil.valueOf(it.getType()))
                                    .nullable(it.getNullable() != null && it.getNullable());
                        }
                    }
                }
        );
        return builder.build();
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return LogicalTableScan.create(context.getCluster(), relOptTable, new ArrayList<>());
    }
}
