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
package io.arenadata.dtm.query.execution.plugin.api.request;

import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;

import java.util.List;

public class LlrRequest extends DatamartRequest {

    private List<Datamart> schema;
    private List<ColumnMetadata> metadata;

    public LlrRequest(QueryRequest queryRequest, List<Datamart> schema, List<ColumnMetadata> metadata) {
        super(queryRequest);
        this.schema = schema;
        this.metadata = metadata;
    }

    public List<Datamart> getSchema() {
        return schema;
    }

    public void setSchema(List<Datamart> schema) {
        this.schema = schema;
    }

    public List<ColumnMetadata> getMetadata() {
        return metadata;
    }

    public void setMetadata(List<ColumnMetadata> metadata) {
        this.metadata = metadata;
    }

    @Override
    public String toString() {
        return "LlrRequest{" +
                "schema=" + schema +
                ", metadata=" + metadata +
                '}';
    }
}
