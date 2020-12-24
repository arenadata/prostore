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
package io.arenadata.dtm.common.plugin.exload;

import com.fasterxml.jackson.annotation.JsonFormat;

@JsonFormat(shape = JsonFormat.Shape.NUMBER_INT)
public enum Format {
  AVRO("avro"),
  CSV("csv"),
  TEXT("text");

  private String name;

  Format(String name) {
    this.name = name;
  }

  public static Format findByName(String name) {
    for (Format value : Format.values()) {
      if (value.name.equalsIgnoreCase(name)) {
        return value;
      }
    }
    throw new IllegalArgumentException("Не найден соответствующий формат для " + name);
  }

  public String getName() {
    return name;
  }
}

