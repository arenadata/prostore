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
package io.arenadata.dtm.query.execution.core.query.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.arenadata.dtm.async.AsyncUtils;
import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.InputQueryRequest;
import io.arenadata.dtm.common.reader.QueryResult;
import io.arenadata.dtm.query.execution.core.query.service.QueryAnalyzer;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.http.HttpHeaders;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.util.MimeTypeUtils;

@Slf4j
@Component
public class QueryController {
    private final QueryAnalyzer queryAnalyzer;
    private final ObjectMapper objectMapper;
    private final DtmConfig dtmSettings;

    @Autowired
    public QueryController(QueryAnalyzer queryAnalyzer,
                           @Qualifier("coreObjectMapper") ObjectMapper objectMapper,
                           DtmConfig dtmSettings) {
        this.queryAnalyzer = queryAnalyzer;
        this.objectMapper = objectMapper;
        this.dtmSettings = dtmSettings;
    }

    public void executeQuery(RoutingContext context) {
        InputQueryRequest inputQueryRequest = context.getBodyAsJson().mapTo(InputQueryRequest.class);
        log.info("Execution request sent: [{}]", inputQueryRequest);
        execute(context, inputQueryRequest);
    }

    public void prepareQuery(RoutingContext context) {
        InputQueryRequest inputQueryRequest = context.getBodyAsJson().mapTo(InputQueryRequest.class);
        inputQueryRequest.setExecutable(false);
        log.info("Request for preparing sent: [{}]", inputQueryRequest);
        execute(context, inputQueryRequest);
    }

    private void execute(RoutingContext context, InputQueryRequest inputQueryRequest) {
        AsyncUtils.measureMs(queryAnalyzer.analyzeAndExecute(inputQueryRequest),
                duration -> log.info("Request succeeded: [{}] in [{}]ms", inputQueryRequest.getSql(), duration))
                .onSuccess(queryResult -> {
                    if (queryResult.getRequestId() == null) {
                        queryResult.setRequestId(inputQueryRequest.getRequestId());
                    }
                    queryResult.setTimeZone(this.dtmSettings.getTimeZone().toString());
                    sendResponse(context, queryResult);
                })
                .onFailure(fail -> {
                    log.error("Error while executing request [{}]", inputQueryRequest, fail);
                    context.fail(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), fail);

                });
    }

    private void sendResponse(RoutingContext context, QueryResult queryResult) {
        try {
            final String json = objectMapper.writeValueAsString(queryResult);
            context.response()
                    .putHeader(HttpHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON_VALUE)
                    .setStatusCode(HttpResponseStatus.OK.code())
                    .end(json);
        } catch (JsonProcessingException e) {
            log.error("Error in serializing query result", e);
            context.fail(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), new DtmException(e));
        }
    }
}
