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
package io.arenadata.dtm.query.execution.core;

import io.arenadata.dtm.query.execution.core.filter.ExcludePluginFilter;
import io.arenadata.dtm.query.execution.core.utils.BeanNameGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.admin.SpringApplicationAdminJmxAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;

@SpringBootApplication(exclude = {SpringApplicationAdminJmxAutoConfiguration.class})
@ConfigurationPropertiesScan("io.arenadata.dtm")
@ComponentScan(
    basePackages = {"io.arenadata.dtm.query.execution", "io.arenadata.dtm.kafka.core"},
    excludeFilters = {@ComponentScan.Filter(type = FilterType.CUSTOM, classes = ExcludePluginFilter.class)},
    nameGenerator = BeanNameGenerator.class
)
public class ServiceQueryExecutionApplication {

    public static void main(String[] args) {
        SpringApplication.run(ServiceQueryExecutionApplication.class, args);
    }
}
