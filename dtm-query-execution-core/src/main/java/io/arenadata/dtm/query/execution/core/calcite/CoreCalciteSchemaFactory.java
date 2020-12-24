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
package io.arenadata.dtm.query.execution.core.calcite;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.query.calcite.core.factory.SchemaFactory;
import io.arenadata.dtm.query.calcite.core.factory.impl.CalciteSchemaFactory;
import io.arenadata.dtm.query.calcite.core.schema.DtmTable;
import io.arenadata.dtm.query.calcite.core.schema.QueryableSchema;
import io.arenadata.dtm.query.execution.core.calcite.schema.CoreDtmTable;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component("coreCalciteSchemaFactory")
public class CoreCalciteSchemaFactory extends CalciteSchemaFactory {
    public CoreCalciteSchemaFactory(@Qualifier("coreSchemaFactory") SchemaFactory schemaFactory) {
        super(schemaFactory);
    }

    @Override
    protected DtmTable createTable(QueryableSchema schema, Entity entity) {
        return new CoreDtmTable(schema, entity);
    }
}
