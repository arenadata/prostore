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
package io.arenadata.dtm.query.execution.plugin.adb.dto;

import io.arenadata.dtm.common.reader.QueryRequest;

public class RegexPreprocessorResult {
  private final QueryRequest originalQueryRequest;
  private String modifiedSql;

  private String systemTimeAsOf;

  public RegexPreprocessorResult(QueryRequest originalQueryRequest) {
    this.originalQueryRequest = originalQueryRequest;
    modifiedSql = originalQueryRequest.getSql().replaceAll("\r\n", " ").replaceAll("\n", " ");
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

  public String getSystemTimeAsOf() {
    return systemTimeAsOf;
  }

  public void setSystemTimeAsOf(String systemTimeAsOf) {
    this.systemTimeAsOf = systemTimeAsOf;
  }

  public QueryRequest getActualQueryRequest() {
    if (!isSqlModified())
      return originalQueryRequest;
    final QueryRequest copy = originalQueryRequest.copy();
    copy.setSql(modifiedSql);
    return copy;
  }
}
