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
package io.arenadata.dtm.jdbc.ext;

import io.arenadata.dtm.jdbc.core.BaseConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class DtmDatabaseMetaDataTest {

    private final BaseConnection connection = mock(DtmConnectionImpl.class);
    private DatabaseMetaData databaseMetaData;

    @BeforeEach
    void setUp() {
        databaseMetaData = new DtmDatabaseMetaData(connection);
    }

    @Test
    void unwrap() throws SQLException {
        assertEquals(databaseMetaData, databaseMetaData.unwrap(DtmDatabaseMetaData.class));
        assertThrows(SQLException.class, () -> databaseMetaData.unwrap(DtmDatabaseMetaDataTest.class));
    }

    @Test
    void isWrapperFor() throws SQLException {
        assertTrue(databaseMetaData.isWrapperFor(DtmDatabaseMetaData.class));
        assertFalse(databaseMetaData.isWrapperFor(null));
    }
}