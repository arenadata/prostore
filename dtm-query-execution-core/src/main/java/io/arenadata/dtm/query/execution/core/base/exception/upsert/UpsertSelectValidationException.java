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
package io.arenadata.dtm.query.execution.core.base.exception.upsert;

import io.arenadata.dtm.common.exception.DtmException;

public class UpsertSelectValidationException extends DtmException {
    private static final String CONFLICT_COLUMN_COUNT_PATTERN = "Upsert select into %s has conflict with query columns wrong count, entity: %d, query: %d";

    private UpsertSelectValidationException(String message) {
        super(message);
    }

    public static UpsertSelectValidationException columnCountConflict(String name, int viewColumns, int queryColumns) {
        return new UpsertSelectValidationException(String.format(CONFLICT_COLUMN_COUNT_PATTERN, name, viewColumns, queryColumns));
    }

    public static UpsertSelectValidationException estimateIsNotAllowed() {
        return new UpsertSelectValidationException("ESTIMATE_ONLY is not allowed in upsert select");
    }
}
