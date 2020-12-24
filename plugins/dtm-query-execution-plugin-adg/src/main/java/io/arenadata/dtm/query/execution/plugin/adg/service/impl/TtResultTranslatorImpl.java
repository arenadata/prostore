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
package io.arenadata.dtm.query.execution.plugin.adg.service.impl;

import io.arenadata.dtm.query.execution.plugin.adg.exception.DtmTarantoolException;
import io.arenadata.dtm.query.execution.plugin.adg.service.TtResultTranslator;
import lombok.val;
import org.tarantool.TarantoolException;

import java.util.List;
import java.util.Map;

public class TtResultTranslatorImpl implements TtResultTranslator {

  @Override
  public List<?> translate(List<?> list) {
    if (list.get(0) != null) {
      return list;
    }
    val error = list.get(1);
    if (error instanceof String) {
      throw new DtmTarantoolException((String) error);
    }
    if (error instanceof Map<?, ?>) {
      // TODO implement this
      // val stack = error["stack"] as String
      // val file = error["file"] as String
      // val className = error["class_name"] as String
      // val line = error["line"] as Int
      val str = (String) ((Map<?, ?>) error).get("str");
      val err = (String) ((Map<?, ?>) error).get("err");
      throw new DtmTarantoolException(0, err, new TarantoolException(0, str));
    } else {
      throw new DtmTarantoolException("Unknown type of error: " + error);
    }
  }
}
