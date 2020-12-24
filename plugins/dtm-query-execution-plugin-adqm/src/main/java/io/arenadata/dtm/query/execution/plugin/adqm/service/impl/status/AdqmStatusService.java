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
package io.arenadata.dtm.query.execution.plugin.adqm.service.impl.status;

import io.arenadata.dtm.common.plugin.status.StatusQueryResult;
import io.arenadata.dtm.kafka.core.service.kafka.KafkaConsumerMonitor;
import io.arenadata.dtm.query.execution.plugin.adqm.dto.StatusReportDto;
import io.arenadata.dtm.query.execution.plugin.adqm.service.StatusReporter;
import io.arenadata.dtm.query.execution.plugin.api.request.StatusRequest;
import io.arenadata.dtm.query.execution.plugin.api.service.StatusService;
import io.arenadata.dtm.query.execution.plugin.api.status.StatusRequestContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service("adqmStatusService")
@Slf4j
public class AdqmStatusService implements StatusService<StatusQueryResult>, StatusReporter {
	private final KafkaConsumerMonitor kafkaConsumerMonitor;
	private final Map<String, String> topicsInUse = new HashMap<>();

	public AdqmStatusService(@Qualifier("coreKafkaConsumerMonitor") KafkaConsumerMonitor kafkaConsumerMonitor) {
		this.kafkaConsumerMonitor = kafkaConsumerMonitor;
	}

	@Override
	public void execute(StatusRequestContext context, Handler<AsyncResult<StatusQueryResult>> handler) {
		if (context == null || context.getRequest() == null) {
			handler.handle(Future.failedFuture("StatusRequestContext should not be null"));
			return;
		}

		StatusRequest request = context.getRequest();

		if (topicsInUse.containsKey(request.getTopic())) {
			String consumerGroup = topicsInUse.get(request.getTopic());

			kafkaConsumerMonitor.getAggregateGroupConsumerInfo(consumerGroup, request.getTopic())
					.onSuccess(p -> {
						StatusQueryResult result = new StatusQueryResult();
						result.setPartitionInfo(p);
						handler.handle(Future.succeededFuture(result));
					})
					.onFailure(f -> handler.handle(Future.failedFuture(f)));
		} else {
			handler.handle(Future.failedFuture("Cannot find info about " + request.getTopic()));
		}
	}

	@Override
	public void onStart(@NonNull final StatusReportDto payload) {
		String topic = payload.getTopic();
		String consumerGroup = payload.getConsumerGroup();
		topicsInUse.put(topic, consumerGroup);
	}

	@Override
	public void onFinish(@NonNull final StatusReportDto payload) {
		topicsInUse.remove(payload.getTopic());
	}

	@Override
	public void onError(@NonNull final StatusReportDto payload) {
		topicsInUse.remove(payload.getTopic());
	}
}
