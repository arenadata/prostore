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
package io.arenadata.dtm.query.execution.core.factory.impl;

import io.arenadata.dtm.query.calcite.core.factory.impl.DtmSchemaFactory;
import io.arenadata.dtm.query.calcite.core.schema.dialect.DtmConvention;
import io.arenadata.dtm.query.execution.core.calcite.schema.dialect.CoreDtmConvention;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import org.apache.calcite.linq4j.tree.Expression;
import org.springframework.stereotype.Component;

@Component("coreSchemaFactory")
public class CoreSchemaFactory extends DtmSchemaFactory {
    @Override
    protected DtmConvention createDtmConvention(Datamart datamart, Expression expression) {
        return new CoreDtmConvention(datamart, expression);
    }
}
