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
package io.arenadata.dtm.query.execution.core.base.service.metadata;

import io.arenadata.dtm.query.execution.core.base.dto.metadata.DatamartEntity;
import io.arenadata.dtm.query.execution.core.base.dto.metadata.DatamartInfo;
import io.arenadata.dtm.query.execution.core.base.dto.metadata.EntityAttribute;
import io.vertx.core.Future;

import java.util.List;

/**
 * Сервис информации о метаданных витрин
 */
public interface DatamartMetaService {

  /**
   * Получение метаданных о всех витринах
   * @return list of datamarts
   */
  Future<List<DatamartInfo>> getDatamartMeta();

  /**
   * Получение метаданных о всех сущностях витрины
   * @return list of entities
   */
  Future<List<DatamartEntity>> getEntitiesMeta(String datamartMnemonic);

  /**
   * Получение метаданных о всех атрибутах сущности
   * @return list of entities attributes
   */
  Future<List<EntityAttribute>> getAttributesMeta(String datamartMnemonic, String entityMnemonic);
}
