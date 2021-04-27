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
package io.arenadata.dtm.query.execution.core.metrics.service.impl;

import io.arenadata.dtm.common.configuration.core.DtmConfig;
import io.arenadata.dtm.common.metrics.RequestMetrics;
import io.arenadata.dtm.query.execution.core.metrics.dto.MetricsSettings;
import io.arenadata.dtm.query.execution.core.metrics.service.AbstractMetricsService;
import io.arenadata.dtm.query.execution.core.metrics.service.MetricsProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("coreMetricsService")
public class MetricsServiceImpl extends AbstractMetricsService<RequestMetrics> {

    @Autowired
    public MetricsServiceImpl(MetricsProducer metricsProducer, DtmConfig dtmSettings, MetricsSettings metricsSettings) {
        super(metricsProducer, dtmSettings, metricsSettings);
    }
}