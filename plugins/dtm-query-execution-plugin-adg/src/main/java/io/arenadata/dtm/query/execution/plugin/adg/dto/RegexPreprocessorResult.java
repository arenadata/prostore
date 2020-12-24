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
package io.arenadata.dtm.query.execution.plugin.adg.dto;

import io.arenadata.dtm.common.reader.QueryRequest;

import java.util.Collections;
import java.util.Map;

public class RegexPreprocessorResult {
  private final QueryRequest originalQueryRequest;
  private String modifiedSql;
  private Map<String, String> systemTimesForTables;

  public RegexPreprocessorResult(QueryRequest originalQueryRequest) {
    this.originalQueryRequest = originalQueryRequest;
    modifiedSql = originalQueryRequest.getSql().replaceAll("\r\n", " ").replaceAll("\n", " ");
    systemTimesForTables = Collections.emptyMap();
  }

  public boolean isSqlModified() {
    return !originalQueryRequest.getSql().equals(modifiedSql);
  }

  public QueryRequest getOriginalQueryRequest() {
    return originalQueryRequest;
  }

  public String getModifiedSql() {
    return modifiedSql;
  }

  public void setModifiedSql(String modifiedSql) {
    this.modifiedSql = modifiedSql;
  }

  public void setSystemTimesForTables(Map<String, String> systemTimesForTables) {
    this.systemTimesForTables = systemTimesForTables;
  }

  public Map<String, String> getSystemTimesForTables() {
    return systemTimesForTables;
  }

  public QueryRequest getActualQueryRequest() {
    if (!isSqlModified())
      return originalQueryRequest;
    final QueryRequest copy = originalQueryRequest.copy();
    copy.setSql(modifiedSql);
    return copy;
  }
}
