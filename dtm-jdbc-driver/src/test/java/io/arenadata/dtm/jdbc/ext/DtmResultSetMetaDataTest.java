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
import io.arenadata.dtm.jdbc.core.Field;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class DtmResultSetMetaDataTest {

    private final BaseConnection connection = mock(DtmConnectionImpl.class);
    private ResultSetMetaData resultSetMetaData;

    @BeforeEach
    void setUp() {
        Field[] fields = new Field[0];
        resultSetMetaData = new DtmResultSetMetaData(connection, fields);
    }

    @Test
    void unwrap() throws SQLException {
        assertEquals(resultSetMetaData, resultSetMetaData.unwrap(DtmResultSetMetaData.class));
        assertThrows(SQLException.class, () -> resultSetMetaData.unwrap(DtmResultSetTest.class));
    }

    @Test
    void isWrapperFor() throws SQLException {
        assertTrue(resultSetMetaData.isWrapperFor(DtmResultSetMetaData.class));
        assertFalse(resultSetMetaData.isWrapperFor(null));
    }
}