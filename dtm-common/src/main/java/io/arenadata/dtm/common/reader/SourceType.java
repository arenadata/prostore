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
package io.arenadata.dtm.common.reader;

import io.arenadata.dtm.common.exception.InvalidSourceTypeException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Data source type
 */

@NoArgsConstructor
@AllArgsConstructor
public enum SourceType {
    ADB,
    ADG,
    ADQM,
    INFORMATION_SCHEMA(false);

    private @Getter
    boolean isAvailable = true;

    public static SourceType valueOfAvailable(String typeName) {
        return Arrays.stream(SourceType.values())
                .filter(type -> type.isAvailable() && type.name().equalsIgnoreCase(typeName))
                .findAny()
                .orElseThrow(() -> new InvalidSourceTypeException(typeName));
    }

    public static Set<SourceType> pluginsSourceTypes() {
        return Arrays.stream(SourceType.values())
                .filter(st -> st != SourceType.INFORMATION_SCHEMA)
                .collect(Collectors.toSet());
    }
}
