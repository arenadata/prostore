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
package io.arenadata.dtm.query.execution.core;

import io.arenadata.dtm.query.execution.core.base.utils.BeanNameGenerator;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.admin.SpringApplicationAdminJmxAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Profile;

/**
 * Отделяем основной контекст CORE от плагинов делая его независимым для тестирования
 */
@Profile("test")
@EnableAutoConfiguration(exclude = {SpringApplicationAdminJmxAutoConfiguration.class})
@ComponentScan(basePackages = {
        "io.arenadata.dtm.query.execution.core.calcite",
        "io.arenadata.dtm.query.execution.core.configuration",
        "io.arenadata.dtm.query.execution.core.converter",
        "io.arenadata.dtm.query.execution.core.dao",
        "io.arenadata.dtm.query.execution.core.factory",
        "io.arenadata.dtm.query.execution.core.registry",
        "io.arenadata.dtm.query.execution.core.service",
        "io.arenadata.dtm.query.execution.core.transformer",
        "io.arenadata.dtm.query.execution.core.utils",
        "io.arenadata.dtm.kafka.core.configuration",
        "io.arenadata.dtm.kafka.core.repository",
        "io.arenadata.dtm.kafka.core.service.kafka"}, nameGenerator = BeanNameGenerator.class)
public class CoreTestConfiguration {
}
