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
package io.arenadata.dtm.query.execution.core.utils;

import io.arenadata.dtm.common.reader.InformationSchemaView;
import io.arenadata.dtm.query.execution.core.dto.metadata.InformationSchemaViewPosition;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Класс для работы с запросами в информационную схему
 */
public class MetaDataQueryPreparer {

  private static final Pattern FIND_TABLE = Pattern.compile(
    "\\s(from|join)\\s+([A-z.0-9\"]+)",
    Pattern.CASE_INSENSITIVE
  );

  /**
   * Найти в запросе информационные представления
   *
   * @param sql запрос
   * @return список позиций представлений в запросе
   */
  public static List<InformationSchemaViewPosition> findInformationSchemaViews(String sql) {
    List<InformationSchemaViewPosition> viewPositions = new ArrayList<>();
    Matcher matcher = FIND_TABLE.matcher(sql);
    while (matcher.find()) {
      InformationSchemaView view = InformationSchemaView.findByFullName(matcher.group(2));
      if (view != null) {
        viewPositions.add(
          new InformationSchemaViewPosition(
            view,
            matcher.start(2),
            matcher.end(2)));
      }
    }
    return viewPositions;
  }

  /**
   * Модифицировать запрос
   *
   * @param sql запрос
   * @return модифицированный запрос
   */
  public static String modify(String sql) {
    List<InformationSchemaViewPosition> viewPositions = findInformationSchemaViews(sql);
    if (CollectionUtils.isEmpty(viewPositions)) {
      return sql;
    }
    viewPositions.sort((vp1, vp2) -> Integer.compare(vp2.getStart(), vp1.getStart()));
    for (InformationSchemaViewPosition viewPosition : viewPositions) {
      sql = sql.substring(0, viewPosition.getStart()) +
        viewPosition.getView().getRealName() + sql.substring(viewPosition.getEnd());
    }
    return sql;
  }
}
