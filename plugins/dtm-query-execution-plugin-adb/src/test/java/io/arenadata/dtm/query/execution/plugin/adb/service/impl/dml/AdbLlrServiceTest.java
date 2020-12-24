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

import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.common.reader.QueryRequest;
import io.arenadata.dtm.query.execution.model.metadata.Datamart;
import io.arenadata.dtm.query.execution.plugin.adb.service.DatabaseExecutor;
import io.arenadata.dtm.query.execution.plugin.adb.service.QueryEnrichmentService;
import io.arenadata.dtm.query.execution.plugin.api.llr.LlrRequestContext;
import io.arenadata.dtm.query.execution.plugin.api.request.LlrRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.LlrService;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestOptions;
import io.vertx.ext.unit.TestSuite;
import io.vertx.ext.unit.report.ReportOptions;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import utils.JsonUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@Slf4j
public class AdbLlrServiceTest {

	private LlrService adbLLRService;
	private QueryEnrichmentService adbQueryEnrichmentService = mock(QueryEnrichmentService.class);
	private DatabaseExecutor adbDatabaseExecutor = mock(DatabaseExecutor.class);

	public AdbLlrServiceTest() {
		//Моки для успешного исполнения
		AsyncResult<Void> asyncResultEmpty = mock(AsyncResult.class);
		when(asyncResultEmpty.succeeded()).thenReturn(true);

		AsyncResult<List<List<?>>> asyncResult = mock(AsyncResult.class);
		when(asyncResult.succeeded()).thenReturn(true);
		when(asyncResult.result()).thenReturn(new ArrayList<>());
		doAnswer((Answer<AsyncResult<Void>>) arg0 -> {
			((Handler<AsyncResult<Void>>) arg0.getArgument(1)).handle(asyncResultEmpty);
			return null;
		}).when(adbQueryEnrichmentService).enrich(any(), any());
		doAnswer((Answer<AsyncResult<List<List<?>>>>) arg0 -> {
			((Handler<AsyncResult<List<List<?>>>>) arg0.getArgument(2)).handle(asyncResult);
			return null;
		}).when(adbDatabaseExecutor).execute(any(), any(), any());

		adbLLRService = new AdbLlrService(adbQueryEnrichmentService, adbDatabaseExecutor);
	}

	@Test
	void executeQuery() {
		List<String> result = new ArrayList<>();
		TestSuite suite = TestSuite.create("the_test_suite");
		suite.test("executeQuery", context -> {
			Async async = context.async();
			JsonObject jsonSchema = JsonUtils.init("meta_data.json", "TEST_DATAMART");
			List<Datamart> schema = new ArrayList<>();
			schema.add(jsonSchema.mapTo(Datamart.class));
			QueryRequest queryRequest = new QueryRequest();
			queryRequest.setSql("SELECT * from PSO");
			queryRequest.setRequestId(UUID.randomUUID());
			queryRequest.setDatamartMnemonic("TEST_DATAMART");
			LlrRequest llrRequest = new LlrRequest(queryRequest, schema, Collections.emptyList());
			adbLLRService.execute(new LlrRequestContext(new RequestMetrics(), llrRequest), ar -> {
				log.debug(ar.toString());
				result.add("OK");
				async.complete();
			});
			async.awaitSuccess(7000);
		});
		suite.run(new TestOptions().addReporter(new ReportOptions().setTo("console")));
		log.info(result.get(0));
	}
}
