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
package io.arenadata.dtm.query.execution.core.base.service.metadata;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.dto.schema.DatamartSchemaKey;
import io.arenadata.dtm.common.model.ddl.Entity;
import io.vertx.core.Future;
import org.apache.calcite.sql.SqlNode;

import java.util.List;
import java.util.Map;

public interface LogicalSchemaService {

    Future<Map<DatamartSchemaKey, Entity>> createSchemaFromQuery(SqlNode sqlNode, String defaultDatamart);

    Future<Map<DatamartSchemaKey, Entity>> createSchemaFromDeltaInformations(List<DeltaInformation> deltaInformations);
}
