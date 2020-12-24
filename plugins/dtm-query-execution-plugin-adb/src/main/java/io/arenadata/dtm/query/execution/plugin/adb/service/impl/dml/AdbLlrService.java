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
package io.arenadata.dtm.query.execution.plugin.adb.service.impl.dml;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

@Service("adbLlrService")
@Slf4j
public class AdbLlrService implements LlrService<QueryResult> {

	private final QueryEnrichmentService adbQueryEnrichmentService;
	private final DatabaseExecutor adbDatabaseExecutor;

	public AdbLlrService(QueryEnrichmentService adbQueryEnrichmentService,
						 @Qualifier("adbQueryExecutor") DatabaseExecutor adbDatabaseExecutor) {
		this.adbQueryEnrichmentService = adbQueryEnrichmentService;
		this.adbDatabaseExecutor = adbDatabaseExecutor;
	}

	@Override
	public void execute(LlrRequestContext context, Handler<AsyncResult<QueryResult>> asyncHandler) {
		LlrRequest request = context.getRequest();
		EnrichQueryRequest enrichQueryRequest = EnrichQueryRequest.generate(request.getQueryRequest(), request.getSchema());
		adbQueryEnrichmentService.enrich(enrichQueryRequest, sqlResult -> {
			if (sqlResult.succeeded()) {
				adbDatabaseExecutor.execute(sqlResult.result(), request.getMetadata(), executeResult -> {
					if (executeResult.succeeded()) {
						asyncHandler.handle(Future.succeededFuture(QueryResult.builder()
							.requestId(request.getQueryRequest().getRequestId())
							.metadata(request.getMetadata())
							.result(executeResult.result())
							.build()));
					} else {
						asyncHandler.handle(Future.failedFuture(executeResult.cause()));
					}
				});
			} else {
				log.error("Error while enriching request");
				asyncHandler.handle(Future.failedFuture(sqlResult.cause()));
			}
		});
	}
}
