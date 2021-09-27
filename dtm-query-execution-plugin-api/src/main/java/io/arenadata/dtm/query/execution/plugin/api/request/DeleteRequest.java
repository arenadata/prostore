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
package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.common.model.ddl.Entity;
import io.arenadata.dtm.common.reader.QueryParameters;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import lombok.Getter;
import org.apache.calcite.sql.SqlDelete;

import java.util.List;
import java.util.UUID;

@Getter
public class DeleteRequest extends LlwRequest<SqlDelete> {

    private final Long deltaOkSysCn;
    private final List<Datamart> datamarts;

    public DeleteRequest(UUID requestId,
                         String envName,
                         String datamartMnemonic,
                         Entity entity,
                         SqlDelete query,
                         Long sysCn,
                         Long deltaOkSysCn,
                         List<Datamart> datamarts,
                         QueryParameters parameters) {
        super(requestId, envName, datamartMnemonic, sysCn, entity, query, parameters);
        this.deltaOkSysCn = deltaOkSysCn;
        this.datamarts = datamarts;
    }
}
