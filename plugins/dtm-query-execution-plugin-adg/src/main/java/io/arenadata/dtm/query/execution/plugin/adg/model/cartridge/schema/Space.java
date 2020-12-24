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
package io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Пространство
 *
 * @format список аттрибутов
 * @temporary временное
 * @engine движок
 * @isLocal локальное
 * @shardingKey ключ шарды
 * @indexes индексы
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Space {
  List<SpaceAttribute> format;
  Boolean temporary;
  SpaceEngines engine;
  @JsonProperty("is_local")
  Boolean isLocal;
  @JsonProperty("sharding_key")
  List<String> shardingKey;
  List<SpaceIndex> indexes;
}
