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
package io.arenadata.dtm.query.execution.plugin.api;

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.query.execution.plugin.api.request.DatamartRequest;
import io.arenadata.dtm.common.model.SqlProcessingType;
import lombok.Data;

@Data
public abstract class RequestContext<Request extends DatamartRequest> {
	private RequestMetrics metrics;
	private Request request;

	public RequestContext(Request request) {
		this.request = request;
	}

	public RequestContext(RequestMetrics metrics, Request request) {
		this.metrics = metrics;
		this.request = request;
	}

	public Request getRequest() {
		return request;
	}

	public RequestMetrics getMetrics() {
		return metrics;
	}

	public void setMetrics(RequestMetrics metrics) {
		this.metrics = metrics;
	}

	public abstract SqlProcessingType getProcessingType();

}
