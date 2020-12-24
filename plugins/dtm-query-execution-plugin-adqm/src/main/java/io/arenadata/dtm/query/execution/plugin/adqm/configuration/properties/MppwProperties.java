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
package io.arenadata.dtm.query.execution.plugin.adqm.configuration.properties;

import io.arenadata.dtm.query.execution.plugin.adqm.service.impl.mppw.load.LoadType;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("adqm.mppw")
@Getter
@Setter
public class MppwProperties {
    private String consumerGroup;
    private String kafkaBrokers;
    private LoadType loadType;
    private String restStartLoadUrl;
    private String restStopLoadUrl;
    private String restLoadConsumerGroup;
}
