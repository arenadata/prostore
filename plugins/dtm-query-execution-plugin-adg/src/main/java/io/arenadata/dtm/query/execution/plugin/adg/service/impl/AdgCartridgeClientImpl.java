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
package io.arenadata.dtm.query.execution.plugin.adg.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.arenadata.dtm.query.execution.plugin.adg.configuration.properties.TarantoolCartridgeProperties;
import io.arenadata.dtm.query.execution.plugin.adg.dto.rollback.ReverseHistoryTransferRequest;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.OperationFile;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.OperationYaml;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.request.*;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.response.*;
import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema.Space;
import io.arenadata.dtm.query.execution.plugin.adg.service.AdgCartridgeClient;
import io.vertx.circuitbreaker.CircuitBreaker;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public class AdgCartridgeClientImpl implements AdgCartridgeClient {
    private static final String STAGE_DATA_TABLE_NAME = "_stage_data_table_name";
    private static final String ACTUAL_DATA_TABLE_NAME = "_actual_data_table_name";
    private static final String HISTORICAL_DATA_TABLE_NAME = "_historical_data_table_name";
    private static final String DELTA_NUMBER = "_delta_number";

    private final WebClient webClient;
    private final TarantoolCartridgeProperties cartridgeProperties;
    private final CircuitBreaker circuitBreaker;
    private final ObjectMapper objectMapper;

    @Autowired
    public AdgCartridgeClientImpl(TarantoolCartridgeProperties cartridgeProperties,
                                  @Qualifier("adgWebClient") WebClient webClient,
                                  @Qualifier("adgCircuitBreaker") CircuitBreaker circuitBreaker,
                                  @Qualifier("yamlMapper") ObjectMapper objectMapper) {
        this.cartridgeProperties = cartridgeProperties;
        this.webClient = webClient;
        this.circuitBreaker = circuitBreaker;
        this.objectMapper = objectMapper;
    }

    @Override
    public void getFiles(Handler<AsyncResult<ResOperation>> handler) {
        executePostRequest(new GetFilesOperation(), handler);
    }

    @Override
    public void setFiles(List<OperationFile> files, Handler<AsyncResult<ResOperation>> handler) {
        executePostRequest(new SetFilesOperation(files), handler);
    }

    @Override
    public void getSchema(Handler<AsyncResult<ResOperation>> handler) {
        executePostRequest(new GetSchemaOperation(), handler);
    }

    @Override
    public void setSchema(String yaml, Handler<AsyncResult<ResOperation>> handler) {
        executePostRequest(new SetSchemaOperation(yaml), handler);
    }

    @Override
    public void uploadData(TtUploadDataKafkaRequest request, Handler<AsyncResult<Void>> handler) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getKafkaUploadDataUrl();
        log.debug("send to [{}] request [{}]", uri, request);
        webClient.postAbs(uri)
            .sendJson(request, ar -> {
                if (ar.succeeded()) {
                    val response = ar.result();
                    handleUploadData(response, handler);
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
    }

    private void handleUploadData(HttpResponse<Buffer> response, Handler<AsyncResult<Void>> handler) {
        try {
            log.trace("handle [UploadData] response [{}]", response);
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                handler.handle(Future.succeededFuture());
                log.debug("UploadData Successful");
            } else if (statusCode == 500) {
                handler.handle(Future.failedFuture(response.bodyAsJson(AdgCartridgeError.class)));
            } else {
                unexpectedResponse(handler, response);
            }
        } catch (Exception ex) {
            handler.handle(Future.failedFuture(ex));
        }
    }

    @Override
    public void subscribe(TtSubscriptionKafkaRequest request, Handler<AsyncResult<Void>> handler) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getKafkaSubscriptionUrl();
        log.debug("send to [{}] request [{}]", uri, request);
        webClient.postAbs(uri)
            .sendJson(request, ar -> {
                if (ar.succeeded()) {
                    val response = ar.result();
                    handleSubscription(response, handler);
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
    }

    private void handleSubscription(HttpResponse<Buffer> response, Handler<AsyncResult<Void>> handler) {
        try {
            log.trace("handle [subscription] response [{}]", response);
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                handler.handle(Future.succeededFuture());
                log.debug("Subscription Successful");
            } else if (statusCode == 500) {
                handler.handle(Future.failedFuture(response.bodyAsJson(AdgCartridgeError.class)));
            } else {
                unexpectedResponse(handler, response);
            }
        } catch (Exception ex) {
            handler.handle(Future.failedFuture(ex));
        }
    }

    @Override
    public void loadData(TtLoadDataKafkaRequest request,
                         Handler<AsyncResult<TtLoadDataKafkaResponse>> handler) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getKafkaLoadDataUrl();
        log.debug("send to [{}] request [{}]", uri, request);
        webClient.postAbs(uri)
            .sendJson(request, ar -> {
                if (ar.succeeded()) {
                    val response = ar.result();
                    handleLoadData(response, handler);
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
    }

    private void handleLoadData(HttpResponse<Buffer> response,
                                Handler<AsyncResult<TtLoadDataKafkaResponse>> handler) {
        try {
            log.trace("handle [load data] response [{}]", response);
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                val successResponse = response.bodyAsJson(TtLoadDataKafkaResponse.class);
                handler.handle(Future.succeededFuture(successResponse));
                log.debug("Loading Successful");
            } else if (statusCode == 500) {
                handler.handle(Future.failedFuture(response.bodyAsJson(TtLoadDataKafkaError.class)));
            } else if (statusCode == 404) {
                handler.handle(Future.failedFuture(response.bodyAsJson(AdgCartridgeError.class)));
            } else {
                unexpectedResponse(handler, response);
            }
        } catch (Exception ex) {
            handler.handle(Future.failedFuture(ex));
        }
    }

    @Override
    public void transferDataToScdTable(TtTransferDataEtlRequest request, Handler<AsyncResult<Void>> handler) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getTransferDataToScdTableUrl();
        log.debug("send to [{}] request [{}]", uri, request);
        val tableNames = request.getHelperTableNames();
        webClient.getAbs(uri)
            .addQueryParam(STAGE_DATA_TABLE_NAME, tableNames.getStaging())
            .addQueryParam(ACTUAL_DATA_TABLE_NAME, tableNames.getActual())
            .addQueryParam(HISTORICAL_DATA_TABLE_NAME, tableNames.getHistory())
            .addQueryParam(DELTA_NUMBER, String.valueOf(request.getDeltaNumber()))
            .send(ar -> {
                if (ar.succeeded()) {
                    val response = ar.result();
                    log.trace("handle [transfer data to scd table] response [{}]", response);
                    val statusCode = response.statusCode();
                    if (statusCode == 200) {
                        handler.handle(Future.succeededFuture());
                    } else if (statusCode == 500) {
                        unexpectedResponse(handler, response);
                    } else {
                        log.error("transfer data to scd table error: {}", response.statusMessage());
                        handler.handle(Future.failedFuture(response.statusMessage()));
                    }
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
    }

    @Override
    public void cancelSubscription(String topicName, Handler<AsyncResult<Void>> handler) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getKafkaSubscriptionUrl() + "/" + topicName;
        log.debug("send to [{}]", uri);
        webClient.deleteAbs(uri)
            .send(ar -> {
                if (ar.succeeded()) {
                    val response = ar.result();
                    handleCancelSubscription(response, handler);
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
    }

    private void handleCancelSubscription(HttpResponse<Buffer> response, Handler<AsyncResult<Void>> handler) {
        try {
            log.trace("handle [cancel subscription] response [{}]", response);
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                handler.handle(Future.succeededFuture());
            } else if (statusCode == 404 || statusCode == 500) {
                handler.handle(Future.failedFuture(response.bodyAsJson(AdgCartridgeError.class)));
            } else {
                unexpectedResponse(handler, response);
            }
        } catch (Exception ex) {
            handler.handle(Future.failedFuture(ex));
        }
    }

    private <T> void unexpectedResponse(Handler<AsyncResult<T>> handler, HttpResponse<Buffer> response) {
        String failureMessage = String.format("Unexpected response %s", response.bodyAsJsonObject());
        handler.handle(Future.failedFuture(failureMessage));
    }

    @SneakyThrows
    private void executeGetRequest(GetRequest request, Handler<AsyncResult<ResStatus>> handler) {
        val httpRequest = webClient.getAbs(cartridgeProperties.getUrl() + request.getRequestUri());
        request.getQueryParamMap().forEach(httpRequest::addQueryParam);
        httpRequest.send(ar -> {
            if (ar.succeeded()) {
                try {
                    val res = new JsonObject(ar.result().body()).mapTo(ResStatus.class);
                    if (ResStatusEnum.ok == res.getStatus()) {
                        handler.handle(Future.succeededFuture(res));
                    } else {
                        handler.handle(Future.failedFuture(String.format("%s: %s", res.getErrorCode(), res.getError())));
                    }
                } catch (Exception e) {
                    handler.handle(Future.failedFuture(e));
                }
            } else {
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    @SneakyThrows
    private void executePostRequest(ReqOperation reqOperation, Handler<AsyncResult<ResOperation>> handler) {
        circuitBreaker.<ResOperation>execute(future -> webClient.postAbs(cartridgeProperties.getUrl() + cartridgeProperties.getAdminApiUrl())
            .sendJson(reqOperation, ar -> {
                if (ar.succeeded()) {
                    try {
                        ResOperation res = new JsonObject(ar.result().body()).mapTo(ResOperation.class);
                        if (CollectionUtils.isEmpty(res.getErrors())) {
                            future.complete(res);
                        } else {
                            future.fail(new RuntimeException(res.getErrors().get(0).getMessage()));
                        }
                    } catch (Exception e) {
                        future.fail(e);
                    }
                } else {
                    future.fail(ar.cause());
                }
            })).setHandler(handler);
    }

    public Future<Map<String, Space>> getSpaceDescriptions(Set<String> spaceNames) {
        Map<String, Set<String>> body = new HashMap<>();
        body.put("spaces", spaceNames);
        String url = cartridgeProperties.getUrl() + cartridgeProperties.getTableSchemaUrl();
        return Future.future(promise -> webClient.postAbs(url)
                .sendJson(body, ar -> {
                    if (ar.succeeded()) {
                        try {
                            promise.complete(parseSpaces(ar.result().bodyAsString(), spaceNames));
                        } catch (Exception e) {
                            promise.fail(e);
                        }
                    } else {
                        promise.fail(ar.cause());
                    }
                }));
    }

    private Map<String, Space> parseSpaces(String yaml, Set<String> spaceNames) throws JsonProcessingException { ;
        JsonNode jsonNode = objectMapper.readTree(yaml).get("spaces");
        return spaceNames.stream()
                .collect(Collectors.toMap(Function.identity(),
                    name -> Optional.ofNullable(jsonNode.get(name))
                        .map(space -> objectMapper.convertValue(space, Space.class))
                        .orElseThrow(() -> new RuntimeException(String.format("`%s` space doesn't exist.", name)))));
    }

    @Override
    public void addSpacesToDeleteQueue(TtDeleteTablesRequest request, Handler<AsyncResult<TtDeleteBatchResponse>> handler) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getTableBatchDeleteUrl();
        log.debug("send to [{}] request [{}]", uri, request);
        webClient.putAbs(uri)
            .sendJson(request, ar -> {
                if (ar.succeeded()) {
                    val response = ar.result();
                    handleAddSpacesToDeleteQueue(response, handler);
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
    }

    private void handleAddSpacesToDeleteQueue(HttpResponse<Buffer> response,
                                              Handler<AsyncResult<TtDeleteBatchResponse>> handler) {
        try {
            log.trace("handle [addSpacesToDeleteQueue] response [{}]", response);
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                val successResponse = response.bodyAsJson(TtDeleteBatchResponse.class);
                handler.handle(Future.succeededFuture(successResponse));
                log.debug("spaces added to delete queue successful");
            } else if (statusCode == 500) {
                handler.handle(Future.failedFuture(response.bodyAsJson(AdgCartridgeError.class)));
            } else {
                unexpectedResponse(handler, response);
            }
        } catch (Exception ex) {
            handler.handle(Future.failedFuture(ex));
        }
    }

    @Override
    public void executeDeleteQueue(TtDeleteTablesQueueRequest request, Handler<AsyncResult<TtDeleteQueueResponse>> handler) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getTableBatchDeleteUrl() + "/"
            + request.getBatchId();
        log.debug("send to [{}] request [{}]", uri, request);
        webClient.deleteAbs(uri)
            .send(ar -> {
                if (ar.succeeded()) {
                    val response = ar.result();
                    handleExecuteDeleteQueue(response, handler);
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
    }

    @Override
    public void executeDeleteSpacesWithPrefix(TtDeleteTablesWithPrefixRequest request,
                                              Handler<AsyncResult<TtDeleteQueueResponse>> handler) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getTableBatchDeleteUrl() + "/prefix/"
            + request.getTablePrefix();
        log.debug("send to [{}] request [{}]", uri, request);
        webClient.deleteAbs(uri)
            .send(ar -> {
                if (ar.succeeded()) {
                    val response = ar.result();
                    handleExecuteDeleteQueue(response, handler);
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
    }

    private void handleExecuteDeleteQueue(HttpResponse<Buffer> response,
                                          Handler<AsyncResult<TtDeleteQueueResponse>> handler) {
        try {
            log.trace("handle [executeDeleteQueue] response [{}]", response);
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                val successResponse = response.bodyAsJson(TtDeleteQueueResponse.class);
                handler.handle(Future.succeededFuture(successResponse));
                log.debug("spaces [{}] dropped successful", successResponse);
            } else if (statusCode == 500) {
                handler.handle(Future.failedFuture(response.bodyAsJson(AdgCartridgeError.class)));
            } else {
                unexpectedResponse(handler, response);
            }
        } catch (Exception ex) {
            handler.handle(Future.failedFuture(ex));
        }
    }

    @Override
    public void reverseHistoryTransfer(ReverseHistoryTransferRequest request, Handler<AsyncResult<Void>> handler) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getReverseHistoryTransferUrl();
        val data = JsonObject.mapFrom(request);
        log.debug("send to [{}] request [{}]", uri, data);
        webClient.postAbs(uri)
            .sendJsonObject(data, ar -> {
                if (ar.succeeded()) {
                    val response = ar.result();
                    handleReverseHistoryTransfer(response, handler);
                } else {
                    handler.handle(Future.failedFuture(ar.cause()));
                }
            });
    }

    private void handleReverseHistoryTransfer(HttpResponse<Buffer> response, Handler<AsyncResult<Void>> handler) {
        try {
            log.trace("handle [reverse history transfer] response [{}]", response);
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                handler.handle(Future.succeededFuture());
            } else if (statusCode == 404 || statusCode == 500) {
                handler.handle(Future.failedFuture(response.bodyAsJson(AdgCartridgeError.class)));
            } else {
                unexpectedResponse(handler, response);
            }
        } catch (Exception ex) {
            handler.handle(Future.failedFuture(ex));
        }
    }

    public void executeCreateSpacesQueued(OperationYaml request, Handler<AsyncResult<Void>> handler) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getTableQueuedCreate();
        webClient.postAbs(uri)
                .sendJson(request, ar -> {
                    if(ar.succeeded()) {
                        val response = ar.result();
                        handleExecuteCreateSpacesQueued(response,handler);
                    } else {
                        handler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    private void handleExecuteCreateSpacesQueued(HttpResponse<Buffer> response, Handler<AsyncResult<Void>> handler) {
        try {
            log.trace("handle [executeCreateSpacesQueued] response [{}]", response);
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                handler.handle(Future.succeededFuture());
            } else if (statusCode == 500) {
                handler.handle(Future.failedFuture(response.bodyAsJson((AdgCartridgeError.class))));
            } else {
                unexpectedResponse(handler, response);
            }
        } catch (Exception ex) {
            handler.handle(Future.failedFuture(ex));
        }
    }

    public void executeDeleteSpacesQueued(TtDeleteTablesRequest request, Handler<AsyncResult<Void>> handler) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getTableQueuedDelete();
        log.debug("send to [{}] request [{}]", uri, request);
        webClient.deleteAbs(uri)
                .sendJson(request,ar -> {
                    if (ar.succeeded()) {
                        val response = ar.result();
                        handleExecuteDeleteSpacesQueued(response,handler);
                    } else {
                        handler.handle(Future.failedFuture(ar.cause()));
                    }
                });
    }

    private void handleExecuteDeleteSpacesQueued(HttpResponse<Buffer> response, Handler<AsyncResult<Void>> handler) {
        try {
            log.trace("handle [executeDeleteSpacesQueued] response [{}]", response);
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                handler.handle(Future.succeededFuture());
            } else if (statusCode == 500) {
                handler.handle(Future.failedFuture(response.bodyAsJson((AdgCartridgeError.class))));
            } else {
                unexpectedResponse(handler, response);
            }
        } catch (Exception ex) {
            handler.handle(Future.failedFuture(ex));
        }
    }

    public void executeDeleteSpacesWithPrefixQueued(TtDeleteTablesWithPrefixRequest request,
                                                    Handler<AsyncResult<Void>> handler) {
        val uri = cartridgeProperties.getUrl() + cartridgeProperties.getTableQueuedDelete() + "/prefix/" +
                request.getTablePrefix();
        log.debug("send to [{}] request [{}]", uri, request);
        webClient.deleteAbs(uri)
                .send(ar -> {
                    if (ar.succeeded()) {
                        val response = ar.result();
                        handleExecuteDeleteSpacesWithPrefixQueued(response,handler);
                    } else {
                        handler.handle(Future.failedFuture(ar.cause()));
                    }
                });

    }

    private void handleExecuteDeleteSpacesWithPrefixQueued(HttpResponse<Buffer> response, Handler<AsyncResult<Void>> handler) {
        try {
            log.trace("handle [executeDeleteSpacesWithPrefixQueued] response [{}]", response);
            val statusCode = response.statusCode();
            if (statusCode == 200) {
                handler.handle(Future.succeededFuture());
            } else if (statusCode == 500) {
                handler.handle(Future.failedFuture(response.bodyAsJson((AdgCartridgeError.class))));
            } else {
                unexpectedResponse(handler, response);
            }
        } catch (Exception ex) {
            handler.handle(Future.failedFuture(ex));
        }
    }

    @Override
    public Future<Long> getCheckSumByInt32Hash(String actualDataTableName,
                                                 String historicalDataTableName,
                                                 Long sysCn,
                                                 Set<String> columnList) {
        Map<String, Object> body = new HashMap<>();
        body.put("actualDataTableName", actualDataTableName);
        body.put("historicalDataTableName", historicalDataTableName);
        body.put("sysCn", sysCn);
        body.put("columnList", columnList);
        String url = cartridgeProperties.getUrl() + cartridgeProperties.getCheckSumUrl();
        return Future.future(promise -> webClient.postAbs(url)
                .sendJson(body, ar -> {
                    if (ar.succeeded()) {
                        JsonObject jsonObject = ar.result().bodyAsJsonObject();
                        Optional<Long> checkSum = Optional.ofNullable(jsonObject.getLong("checksum"));
                        if (checkSum.isPresent()) {
                            promise.complete(checkSum.get());
                        } else {
                            promise.fail(jsonObject.getString("error"));
                        }
                    } else {
                        promise.fail(ar.cause());
                    }
                }));
    }

    public Future<Void> deleteSpaceTuples(String spaceName, String whereCondition) {
        String url = cartridgeProperties.getUrl() + cartridgeProperties.getDeleteSpaceTuples();
        Map<String, String> body = new HashMap<>();
        body.put("spaceName", spaceName);
        body.put("whereCondition", whereCondition);
        return Future.future(promise -> webClient.postAbs(url)
                .sendJson(body, ar -> {
                    if(ar.succeeded()) {
                        JsonObject jsonObject = ar.result().bodyAsJsonObject();
                        if (jsonObject == null) {
                            promise.complete();
                        } else {
                            promise.fail(jsonObject.getString("error"));
                        }
                    } else {
                        promise.fail(ar.cause());
                    }
                }));
    }
}
