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
package io.arenadata.dtm.query.execution.core.service.metadata;

import io.arenadata.dtm.query.execution.core.dto.metadata.DatamartEntity;
import io.arenadata.dtm.query.execution.core.dto.metadata.DatamartInfo;
import io.arenadata.dtm.query.execution.core.dto.metadata.EntityAttribute;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.List;

/**
 * Сервис информации о метаданных витрин
 */
public interface DatamartMetaService {

  /**
   * Получение метаданных о всех витринах
   */
  void getDatamartMeta(Handler<AsyncResult<List<DatamartInfo>>> resultHandler);

  /**
   * Получение метаданных о всех сущностях витрины
   */
  void getEntitiesMeta(String datamartMnemonic, Handler<AsyncResult<List<DatamartEntity>>> resultHandler);

  /**
   * Получение метаданных о всех атрибутах сущности
   */
  void getAttributesMeta(String datamartMnemonic, String entityMnemonic, Handler<AsyncResult<List<EntityAttribute>>> resultHandler);
}
