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
package io.arenadata.dtm.query.execution.plugin.api.mppw.kafka;

import io.arenadata.dtm.common.plugin.exload.Format;
import io.arenadata.dtm.query.execution.plugin.api.edml.BaseExternalEntityMetadata;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class UploadExternalEntityMetadata extends BaseExternalEntityMetadata {
    private Integer uploadMessageLimit;

    @Builder
    public UploadExternalEntityMetadata(String name, String locationPath,
                                        Format format, String externalSchema, Integer uploadMessageLimit) {
        super(name, locationPath, format, externalSchema);
        this.uploadMessageLimit = uploadMessageLimit;
    }
}
