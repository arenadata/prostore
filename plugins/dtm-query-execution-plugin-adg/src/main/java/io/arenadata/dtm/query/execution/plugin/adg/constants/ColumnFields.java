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
package io.arenadata.dtm.query.execution.plugin.adg.constants;

public class ColumnFields {

  /** Идентификатор */
  public final static String ID = "id";
  /** Идентификатор для навигации по запросов с роутера */
  public final static String BUCKET_ID = "bucket_id";
  /** Название текущей таблицы */
  public static final String ACTUAL_POSTFIX = "_actual";
  /** Название staging таблицы */
  public static final String STAGING_POSTFIX = "_staging";
  /** Название таблицы истории */
  public static final String HISTORY_POSTFIX = "_history";
  /** Системное поле операции над объектом */
  public static final String SYS_OP_FIELD = "sys_op";
  /** Системное поле номера дельты */
  public static final String SYS_FROM_FIELD = "sys_from";
  /** Системное поле максимального номера дельты */
  public static final String SYS_TO_FIELD = "sys_to";
}
