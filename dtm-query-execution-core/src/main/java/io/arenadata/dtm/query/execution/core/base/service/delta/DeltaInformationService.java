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
package io.arenadata.dtm.query.execution.core.base.service.delta;

import io.arenadata.dtm.common.delta.SelectOnInterval;
import io.vertx.core.Future;

import java.time.LocalDateTime;

/**
 * Delta processing service
 */
public interface DeltaInformationService {

    Future<Long> getCnToByDeltaDatetime(String datamart, LocalDateTime dateTime);

    Future<Long> getDeltaNumByDatetime(String datamart, LocalDateTime dateTime);

    Future<Long> getCnToByDeltaNum(String datamart, long num);

    Future<Long> getCnToDeltaHot(String datamart);

    Future<SelectOnInterval> getCnFromCnToByDeltaNums(String datamart, long deltaFrom, long deltaTo);

    Future<Long> getCnToDeltaOk(String datamart);
}
