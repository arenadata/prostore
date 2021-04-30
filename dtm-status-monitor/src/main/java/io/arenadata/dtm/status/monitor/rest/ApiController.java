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
package io.arenadata.dtm.status.monitor.rest;

import io.arenadata.dtm.common.status.kafka.StatusRequest;
import io.arenadata.dtm.common.status.kafka.StatusResponse;
import io.arenadata.dtm.common.version.VersionInfo;
import io.arenadata.dtm.status.monitor.kafka.KafkaMonitor;
import io.arenadata.dtm.status.monitor.version.VersionService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/")
public class ApiController {
    private final KafkaMonitor kafkaMonitor;
    private final VersionService versionService;

    public ApiController(KafkaMonitor kafkaMonitor, VersionService versionService) {
        this.kafkaMonitor = kafkaMonitor;
        this.versionService = versionService;
    }

    @PostMapping("/status")
    public StatusResponse status(@RequestBody StatusRequest request) {
        return kafkaMonitor.status(request);
    }

    @GetMapping("/versions")
    public VersionInfo version() {
        return versionService.getVersionInfo();
    }
}
