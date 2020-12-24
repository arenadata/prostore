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
package io.arenadata.dtm.query.execution.plugin.adb.service.impl.mppr;

import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.plugin.adb.dto.EnrichQueryRequest;
import io.arenadata.dtm.query.execution.plugin.adb.factory.MetadataSqlFactory;
import io.arenadata.dtm.query.execution.plugin.adb.factory.impl.MetadataSqlFactoryImpl;
import io.arenadata.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.adb.service.impl.query.AdbQueryExecutor;
import io.arenadata.dtm.query.execution.plugin.api.mppr.MpprRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.MpprRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.MpprKafkaService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.stereotype.Service;

@Slf4j
@Service("adbMpprKafkaService")
public class AdbMpprKafkaService implements MpprKafkaService<QueryResult> {

	private final QueryEnrichmentService adbQueryEnrichmentService;
	private final MetadataSqlFactory metadataSqlFactory;
	private final AdbQueryExecutor adbQueryExecutor;

	public AdbMpprKafkaService(QueryEnrichmentService adbQueryEnrichmentService,
							   MetadataSqlFactory metadataSqlFactory,
							   AdbQueryExecutor adbQueryExecutor) {
		this.adbQueryEnrichmentService = adbQueryEnrichmentService;
		this.metadataSqlFactory = metadataSqlFactory;
		this.adbQueryExecutor = adbQueryExecutor;
	}

	@Override
	public void execute(MpprRequestContext context, Handler<AsyncResult<QueryResult>> asyncHandler) {
		val request = context.getRequest();
		val schema = request.getQueryRequest().getDatamartMnemonic();
		val table = MetadataSqlFactoryImpl.WRITABLE_EXTERNAL_TABLE_PREF + request.getQueryRequest().getRequestId().toString().replaceAll("-", "_");
		createWritableExtTable(request)
				.compose(v -> getEnrichedQuery(request))
				.compose(sql -> executeInsert(schema, table, sql))
				.compose(v -> dropWritableExtTable(schema, table))
				.onSuccess(success -> asyncHandler.handle(Future.succeededFuture(QueryResult.emptyResult())))
				.onFailure(err -> {
					log.error("Failed to unload data from ADB: %s on request %s", err.getMessage(), request.getQueryRequest().getRequestId());
					dropWritableExtTable(schema, table)
							.onComplete(dropResult -> {
								if (dropResult.failed()) {
									log.error("Failed to drop writable external table {}.{}", schema, table);
								}
								asyncHandler.handle(Future.failedFuture(err));
							});
				});
	}

	private Future<String> getEnrichedQuery(MpprRequest request) {
		return Future.future(promise -> adbQueryEnrichmentService.enrich(
				EnrichQueryRequest.generate(request.getQueryRequest(), request.getLogicalSchema()), promise));
	}

	private Future<Void> createWritableExtTable(MpprRequest request){
		return Future.future(p -> adbQueryExecutor.executeUpdate(metadataSqlFactory.createWritableExtTableSqlQuery(request), p));
	}

	private Future<Void> executeInsert(String schema, String table, String enrichedSql) {
		return Future.future(p -> {
			adbQueryExecutor.executeUpdate(metadataSqlFactory.insertIntoWritableExtTableSqlQuery(schema, table, enrichedSql), p);
		});
	}

	private Future<Void> dropWritableExtTable(String schema, String table) {
		return Future.future(p -> {
			adbQueryExecutor.executeUpdate(metadataSqlFactory.dropWritableExtTableSqlQuery(schema, table), p);
		});
	}


}
