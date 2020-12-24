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
package io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.request;

import io.arenadata.dtm.query.execution.plugin.adg.model.cartridge.variable.YamlVariables;

/**
 * Установить схему
 */
public class SetSchemaOperation extends ReqOperation {

  public SetSchemaOperation(String yaml) {
    super("set_schema", new YamlVariables(yaml),
      "mutation set_schema($yaml: String!) {\n" +
      " cluster { schema(as_yaml: $yaml) { as_yaml }}\n" +
      "}");
  }
}
