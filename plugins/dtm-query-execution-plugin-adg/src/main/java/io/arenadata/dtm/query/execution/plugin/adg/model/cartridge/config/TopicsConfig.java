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
package io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Конфигурации топиков
 *
 * @error_topic топик ошибок
 * @schema_key название схемы
 * @target_table таблица
 * @schema_data схема из Schema Registry
 * @success_topic топик успешного выполнения запроса
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopicsConfig {

  public static final String FILE_NAME = "kafka.topics.yml";

  @JsonProperty("error_topic")
  String errorTopic;
  @JsonProperty("schema_key")
  String schemaKey;
  @JsonProperty("target_table")
  String targetTable;
  @JsonProperty("schema_data")
  String schemaData;
  @JsonProperty("success_topic")
  String successTopic;
}
