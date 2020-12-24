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
package io.arenadata.dtm.query.execution.plugin.api.dml;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.query.execution.plugin.api.RequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.arenadata.dtm.common.model.SqlProcessingType;
import lombok.ToString;
import org.apache.calcite.sql.SqlNode;

import static io.arenadata.dtm.common.model.SqlProcessingType.DML;

@ToString
public class DmlRequestContext extends RequestContext<DatamartRequest> {

	private SqlNode query;

	public DmlRequestContext(RequestMetrics metrics, DatamartRequest request, SqlNode query) {
		super(metrics, request);
		this.query = query;
	}

	@Override
	public SqlProcessingType getProcessingType() {
		return DML;
	}

	public SqlNode getQuery() {
		return query;
	}

}
