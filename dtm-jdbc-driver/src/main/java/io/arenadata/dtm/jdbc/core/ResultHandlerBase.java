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
package io.arenadata.dtm.jdbc.core;

import io.arenadata.dtm.query.execution.model.metadata.ColumnMetadata;

import java.sql.SQLException;
import java.sql.SQLWarning;
import java.time.ZoneId;
import java.util.List;

public class ResultHandlerBase implements ResultHandler {

    private SQLException firstException;
    private SQLException lastException;
    private SQLWarning firstWarning;
    private SQLWarning lastWarning;

    public ResultHandlerBase() {
    }

    @Override
    public void handleResultRows(Query query, List<Field[]> fields, List<ColumnMetadata> metadata, ZoneId timeZone) {

    }

    @Override
    public void handleWarning(SQLWarning warning) {
        if (this.firstWarning == null) {
            this.firstWarning = this.lastWarning = warning;
        } else {
            SQLWarning lastWarning = this.lastWarning;
            lastWarning.setNextException(warning);
            this.lastWarning = warning;
        }
    }

    @Override
    public void handleError(SQLException sqlException) {
        if (this.firstException == null) {
            this.firstException = this.lastException = sqlException;
        } else {
            this.lastException.setNextException(sqlException);
            this.lastException = sqlException;
        }
    }

    @Override
    public SQLException getException() {
        return this.firstException;
    }

    @Override
    public SQLWarning getWarning() {
        return this.firstWarning;
    }
}
