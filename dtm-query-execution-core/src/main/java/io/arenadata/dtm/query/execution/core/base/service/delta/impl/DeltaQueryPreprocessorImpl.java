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
package io.arenadata.dtm.query.execution.core.base.service.delta.impl;

import io.arenadata.dtm.common.delta.DeltaInformation;
import io.arenadata.dtm.common.delta.DeltaType;
import io.arenadata.dtm.common.exception.DeltaRangeInvalidException;
import io.arenadata.dtm.common.exception.DtmException;
import io.arenadata.dtm.common.reader.InformationSchemaView;
import io.arenadata.dtm.query.calcite.core.dto.delta.DeltaQueryPreprocessorResponse;
import io.arenadata.dtm.query.calcite.core.util.CalciteUtil;
import io.arenadata.dtm.query.execution.core.base.service.delta.DeltaInformationExtractor;
import io.arenadata.dtm.query.execution.core.base.service.delta.DeltaInformationService;
import io.arenadata.dtm.query.execution.core.base.service.delta.DeltaQueryPreprocessor;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.calcite.sql.SqlNode;

import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class DeltaQueryPreprocessorImpl implements DeltaQueryPreprocessor {
    private final DeltaInformationExtractor deltaInformationExtractor;
    private final DeltaInformationService deltaService;

    public DeltaQueryPreprocessorImpl(DeltaInformationService deltaService,
                                      DeltaInformationExtractor deltaInformationExtractor) {
        this.deltaService = deltaService;
        this.deltaInformationExtractor = deltaInformationExtractor;
    }

    @Override
    public Future<DeltaQueryPreprocessorResponse> process(SqlNode request) {
        if (request == null) {
            log.error("Request is empty");
            return Future.failedFuture(new DtmException("Undefined request"));
        }

        return Future.future(p -> {
            val deltaInfoRes = deltaInformationExtractor.extract(request);
            calculateDeltaValues(deltaInfoRes.getDeltaInformations())
                    .map(deltaInformationList -> new DeltaQueryPreprocessorResponse(deltaInformationList, deltaInfoRes.getSqlWithoutSnapshots(), isCacheable(deltaInformationList)))
                    .onComplete(p);
        });
    }

    private Future<List<DeltaInformation>> calculateDeltaValues(List<DeltaInformation> deltas) {
        return Future.future(promise -> {
            val errors = new HashSet<String>();
            CompositeFuture.join(deltas.stream()
                            .map(deltaInformation -> getCalculateDeltaInfoFuture(deltaInformation)
                                    .onFailure(event -> errors.add(event.getMessage())))
                            .collect(Collectors.toList()))
                    .onSuccess(result -> promise.complete(result.list()))
                    .onFailure(error -> promise.fail(new DeltaRangeInvalidException(String.join(";", errors))));
        });
    }

    private Future<DeltaInformation> getCalculateDeltaInfoFuture(DeltaInformation deltaInformation) {
        if (InformationSchemaView.SCHEMA_NAME.equalsIgnoreCase(deltaInformation.getSchemaName())) {
            return Future.succeededFuture(deltaInformation);
        }

        if (deltaInformation.isLatestUncommittedDelta()) {
            return deltaService.getCnToDeltaHot(deltaInformation.getSchemaName())
                    .map(deltaCnTo -> {
                        deltaInformation.setSelectOnNum(deltaCnTo);
                        return deltaInformation;
                    });
        }

        if (DeltaType.FINISHED_IN.equals(deltaInformation.getType()) || DeltaType.STARTED_IN.equals(deltaInformation.getType())) {
            return getDeltaInformationFromInterval(deltaInformation);
        }

        return getDeltaInformationFromNum(deltaInformation);
    }

    private Future<DeltaInformation> getDeltaInformationFromInterval(DeltaInformation deltaInformation) {
        return Future.future(promise -> {
            val deltaFrom = deltaInformation.getSelectOnInterval().getSelectOnFrom();
            val deltaTo = deltaInformation.getSelectOnInterval().getSelectOnTo();
            deltaService.getCnFromCnToByDeltaNums(deltaInformation.getSchemaName(), deltaFrom, deltaTo)
                    .onSuccess(selectOnInterval -> {
                        deltaInformation.setSelectOnInterval(selectOnInterval);
                        promise.complete(deltaInformation);
                    })
                    .onFailure(promise::fail);
        });
    }

    private Future<DeltaInformation> getDeltaInformationFromNum(DeltaInformation deltaInformation) {
        return Future.future(promise ->
                calculateSelectOnNum(deltaInformation)
                        .onSuccess(cnTo -> {
                            deltaInformation.setSelectOnNum(cnTo);
                            promise.complete(deltaInformation);
                        })
                        .onFailure(promise::fail));
    }

    private Future<Long> calculateSelectOnNum(DeltaInformation deltaInformation) {
        switch (deltaInformation.getType()) {
            case NUM:
                return deltaService.getCnToByDeltaNum(deltaInformation.getSchemaName(), deltaInformation.getSelectOnNum());
            case DATETIME:
                val localDateTime = deltaInformation.getDeltaTimestamp().replace("\'", "");
                return deltaService.getCnToByDeltaDatetime(deltaInformation.getSchemaName(), CalciteUtil.parseLocalDateTime(localDateTime));
            case WITHOUT_SNAPSHOT:
                return deltaService.getCnToDeltaOk(deltaInformation.getSchemaName());
            default:
                return Future.failedFuture(new UnsupportedOperationException("Delta type not supported"));
        }
    }

    private boolean isCacheable(List<DeltaInformation> deltaInformationList) {
        return deltaInformationList.stream().noneMatch(DeltaInformation::isLatestUncommittedDelta);
    }
}
