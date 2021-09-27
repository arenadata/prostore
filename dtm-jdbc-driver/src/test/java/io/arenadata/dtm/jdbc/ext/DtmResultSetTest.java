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

import io.arenadata.dtm.common.configuration.core.CoreConstants;
import io.arenadata.dtm.common.model.ddl.ColumnType;
import io.arenadata.dtm.jdbc.core.BaseConnection;
import io.arenadata.dtm.jdbc.core.BaseStatement;
import io.arenadata.dtm.jdbc.core.Field;
import io.arenadata.dtm.jdbc.core.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class DtmResultSetTest {
    private final BaseConnection connection = mock(DtmConnectionImpl.class);
    private final BaseStatement statement = mock(DtmStatement.class);
    private ResultSet resultSet;
    private TimeZone INITIAL_TIMEZONE;

    @BeforeEach
    void setUp() {
        INITIAL_TIMEZONE = TimeZone.getDefault();
    }

    @AfterEach
    void cleanUp() {
        TimeZone.setDefault(INITIAL_TIMEZONE);
    }

    @Test
    void unwrap() throws SQLException {
        // arrange
        prepareResultSet(Collections.emptyList(), Collections.emptyList());

        // act assert
        assertEquals(resultSet, resultSet.unwrap(DtmResultSet.class));
        assertThrows(SQLException.class, () -> resultSet.unwrap(DtmResultSetTest.class));
    }

    @Test
    void isWrapperFor() throws SQLException {
        // arrange
        prepareResultSet(Collections.emptyList(), Collections.emptyList());

        // act assert
        assertTrue(resultSet.isWrapperFor(DtmResultSet.class));
        assertFalse(resultSet.isWrapperFor(null));
    }

    @Test
    void shouldBeSameDatesWhenUTC() throws SQLException {
        // arrange
        List<LocalDate> localDates = Arrays.asList(
                LocalDate.of(2020, 1, 1),
                LocalDate.of(1990, 6, 6),
                LocalDate.of(1900, 1, 1),
                LocalDate.of(1500, 5, 12),
                LocalDate.of(1000, 6, 11),
                LocalDate.of(123, 11, 20)
        );

        List<Tuple> tuples = localDates.stream()
                .map(localDate -> new Tuple(new Object[]{localDate.toEpochDay()}))
                .collect(Collectors.toList());

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        prepareResultSet(Arrays.asList(new Field("date", ColumnType.DATE)), tuples);

        // act
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            Date date = resultSet.getDate("date");
            result.add(date.toString());
        }

        // assert
        assertThat(result, contains(
                is("2020-01-01"),
                is("1990-06-06"),
                is("1900-01-01"),
                is("1500-05-12"),
                is("1000-06-11"),
                is("0123-11-20")
        ));
    }

    @Test
    void shouldBeSameDatesWhenClientIsShiftedGMT() throws SQLException {
        // arrange
        List<LocalDate> localDates = Arrays.asList(
                LocalDate.of(2020, 1, 1),
                LocalDate.of(1990, 6, 6),
                LocalDate.of(1900, 1, 1),
                LocalDate.of(1500, 5, 12),
                LocalDate.of(1000, 6, 11),
                LocalDate.of(123, 11, 20)
        );

        List<Tuple> tuples = localDates.stream()
                .map(localDate -> new Tuple(new Object[]{localDate.toEpochDay()}))
                .collect(Collectors.toList());

        prepareResultSet(Arrays.asList(new Field("date", ColumnType.DATE)), tuples);

        TimeZone timeZone = TimeZone.getTimeZone("GMT+11:30");
        TimeZone.setDefault(timeZone);

        // act
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            Date date = resultSet.getDate("date", Calendar.getInstance(timeZone));
            result.add(date.toString());
        }

        // assert
        assertThat(result, contains(
                is("2020-01-01"),
                is("1990-06-06"),
                is("1900-01-01"),
                is("1500-05-12"),
                is("1000-06-11"),
                is("0123-11-20")
        ));
    }

    @Test
    void shouldBeSameDatesWhenClientIsNotUtc() throws SQLException {
        // arrange
        List<LocalDate> localDates = Arrays.asList(
                LocalDate.of(2020, 1, 1),
                LocalDate.of(1990, 6, 6),
                LocalDate.of(1900, 1, 1),
                LocalDate.of(1500, 5, 12),
                LocalDate.of(1000, 6, 11),
                LocalDate.of(123, 11, 20)
        );

        List<Tuple> tuples = localDates.stream()
                .map(localDate -> new Tuple(new Object[]{localDate.toEpochDay()}))
                .collect(Collectors.toList());

        prepareResultSet(Arrays.asList(new Field("date", ColumnType.DATE)), tuples);

        TimeZone timeZone = TimeZone.getTimeZone("Asia/Novosibirsk");
        TimeZone.setDefault(timeZone);

        // act
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            Date date = resultSet.getDate("date", Calendar.getInstance(timeZone));
            result.add(date.toString());
        }

        // assert
        assertThat(result, contains(
                is("2020-01-01"),
                is("1990-06-06"),
                is("1900-01-01"),
                is("1500-05-12"),
                is("1000-06-11"),
                is("0123-11-20")
        ));
    }

    @Test
    void shouldBeSameTimestampsWhenUTC() throws SQLException {
        // arrange
        List<LocalDateTime> localDates = Arrays.asList(
                LocalDateTime.of(2020, 1, 1, 11, 11, 11, 123456789),
                LocalDateTime.of(1990, 6, 6, 11, 11, 11, 123456789),
                LocalDateTime.of(1900, 1, 1, 11, 11, 11, 123456789),
                LocalDateTime.of(1500, 5, 12, 11, 11, 11, 123456789),
                LocalDateTime.of(1000, 6, 11, 11, 11, 11, 123456789),
                LocalDateTime.of(123, 11, 20, 11, 11, 11, 123456789)
        );

        List<Tuple> tuples = localDates.stream()
                .map(date -> {
                    Instant instant = date.atZone(CoreConstants.CORE_TIME_ZONE.toZoneId()).toInstant();
                    return new Tuple(new Object[]{instant.getLong(ChronoField.INSTANT_SECONDS) * 1000L * 1000L + instant.getLong(ChronoField.MICRO_OF_SECOND)});
                })
                .collect(Collectors.toList());

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        prepareResultSet(Arrays.asList(new Field("timestamp", 6, ColumnType.TIMESTAMP, null)), tuples);

        // act
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            Timestamp timestamp = resultSet.getTimestamp("timestamp");
            result.add(timestamp.toString());
        }

        // assert
        assertThat(result, contains(
                is("2020-01-01 11:11:11.123456"),
                is("1990-06-06 11:11:11.123456"),
                is("1900-01-01 11:11:11.123456"),
                is("1500-05-12 11:11:11.123456"),
                is("1000-06-11 11:11:11.123456"),
                is("0123-11-20 11:11:11.123456")
        ));
    }

    @Test
    void shouldBeSameTimestampsWhenShiftedGMT() throws SQLException {
        // arrange
        List<LocalDateTime> localDates = Arrays.asList(
                LocalDateTime.of(2020, 1, 1, 11, 11, 11, 123456789),
                LocalDateTime.of(1990, 6, 6, 11, 11, 11, 123456789),
                LocalDateTime.of(1900, 1, 1, 11, 11, 11, 123456789),
                LocalDateTime.of(1500, 5, 12, 11, 11, 11, 123456789),
                LocalDateTime.of(1000, 6, 11, 11, 11, 11, 123456789),
                LocalDateTime.of(123, 11, 20, 11, 11, 11, 123456789)
        );

        List<Tuple> tuples = localDates.stream()
                .map(date -> {
                    Instant instant = date.atZone(CoreConstants.CORE_TIME_ZONE.toZoneId()).toInstant();
                    return new Tuple(new Object[]{instant.getLong(ChronoField.INSTANT_SECONDS) * 1000L * 1000L + instant.getLong(ChronoField.MICRO_OF_SECOND)});
                })
                .collect(Collectors.toList());

        TimeZone.setDefault(TimeZone.getTimeZone("GMT+11:30"));
        prepareResultSet(Arrays.asList(new Field("timestamp", 6, ColumnType.TIMESTAMP, null)), tuples);

        // act
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            Timestamp timestamp = resultSet.getTimestamp("timestamp");
            result.add(timestamp.toString());
        }

        // assert
        assertThat(result, contains(
                is("2020-01-01 11:11:11.123456"),
                is("1990-06-06 11:11:11.123456"),
                is("1900-01-01 11:11:11.123456"),
                is("1500-05-12 11:11:11.123456"),
                is("1000-06-11 11:11:11.123456"),
                is("0123-11-20 11:11:11.123456")
        ));
    }

    @Test
    void shouldBeSameTimestampsWhenNotUTC() throws SQLException {
        // arrange
        List<LocalDateTime> localDates = Arrays.asList(
                LocalDateTime.of(2020, 1, 1, 11, 11, 11, 123456789),
                LocalDateTime.of(1990, 6, 6, 11, 11, 11, 123456789),
                LocalDateTime.of(1900, 1, 1, 11, 11, 11, 123456789),
                LocalDateTime.of(1500, 5, 12, 11, 11, 11, 123456789),
                LocalDateTime.of(1000, 6, 11, 11, 11, 11, 123456789),
                LocalDateTime.of(123, 11, 20, 11, 11, 11, 123456789)
        );

        List<Tuple> tuples = localDates.stream()
                .map(date -> {
                    Instant instant = date.atZone(CoreConstants.CORE_TIME_ZONE.toZoneId()).toInstant();
                    return new Tuple(new Object[]{instant.getLong(ChronoField.INSTANT_SECONDS) * 1000L * 1000L + instant.getLong(ChronoField.MICRO_OF_SECOND)});
                })
                .collect(Collectors.toList());

        prepareResultSet(Arrays.asList(new Field("timestamp", 6, ColumnType.TIMESTAMP, null)), tuples);

        TimeZone timeZone = TimeZone.getTimeZone("Asia/Novosibirsk");
        TimeZone.setDefault(timeZone);

        // act
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            Timestamp timestamp = resultSet.getTimestamp("timestamp", Calendar.getInstance(timeZone));
            result.add(timestamp.toString());
        }

        // assert
        assertThat(result, contains(
                is("2020-01-01 11:11:11.123456"),
                is("1990-06-06 11:11:11.123456"),
                is("1900-01-01 11:11:11.123456"),
                is("1500-05-12 11:11:11.123456"),
                is("1000-06-11 11:11:11.123456"),
                is("0123-11-20 11:11:11.123456")
        ));
    }

    @Test
    void shouldBeSameTimeWhenUTC() throws SQLException {
        // arrange
        List<LocalTime> localDates = Arrays.asList(
                LocalTime.of(1, 1, 11),
                LocalTime.of(5, 5, 11),
                LocalTime.of(9, 9, 11),
                LocalTime.of(12, 12, 30),
                LocalTime.of(16, 16, 40),
                LocalTime.of(23, 23, 50)
        );

        List<Tuple> tuples = localDates.stream()
                .map(date -> new Tuple(new Object[]{date.toNanoOfDay() / 1000}))
                .collect(Collectors.toList());

        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        prepareResultSet(Arrays.asList(new Field("time", ColumnType.TIMESTAMP)), tuples);

        // act
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            Time timestamp = resultSet.getTime("time");
            result.add(timestamp.toString());
        }

        // assert
        assertThat(result, contains(
                is("01:01:11"),
                is("05:05:11"),
                is("09:09:11"),
                is("12:12:30"),
                is("16:16:40"),
                is("23:23:50")
        ));
    }

    @Test
    void shouldBeSameTimeWhenShiftedGMT() throws SQLException {
        // arrange
        List<LocalTime> localDates = Arrays.asList(
                LocalTime.of(1, 1, 11),
                LocalTime.of(5, 5, 11),
                LocalTime.of(9, 9, 11),
                LocalTime.of(12, 12, 30),
                LocalTime.of(16, 16, 40),
                LocalTime.of(23, 23, 50)
        );

        List<Tuple> tuples = localDates.stream()
                .map(date -> new Tuple(new Object[]{date.toNanoOfDay() / 1000}))
                .collect(Collectors.toList());
        prepareResultSet(Arrays.asList(new Field("time", ColumnType.TIMESTAMP)), tuples);

        TimeZone timeZone = TimeZone.getTimeZone("GMT+11:30");
        TimeZone.setDefault(timeZone);

        // act
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            Time timestamp = resultSet.getTime("time", Calendar.getInstance(timeZone));
            result.add(timestamp.toString());
        }

        // assert
        assertThat(result, contains(
                is("01:01:11"),
                is("05:05:11"),
                is("09:09:11"),
                is("12:12:30"),
                is("16:16:40"),
                is("23:23:50")
        ));
    }

    @Test
    void shouldBeSameTimeWhenNotUTC() throws SQLException {
        // arrange
        List<LocalTime> localDates = Arrays.asList(
                LocalTime.of(1, 1, 11),
                LocalTime.of(5, 5, 11),
                LocalTime.of(9, 9, 11),
                LocalTime.of(12, 12, 30),
                LocalTime.of(16, 16, 40),
                LocalTime.of(23, 23, 50)
        );

        List<Tuple> tuples = localDates.stream()
                .map(date -> new Tuple(new Object[]{date.toNanoOfDay() / 1000}))
                .collect(Collectors.toList());
        prepareResultSet(Arrays.asList(new Field("time", ColumnType.TIMESTAMP)), tuples);

        TimeZone timeZone = TimeZone.getTimeZone("Asia/Novosibirsk");
        TimeZone.setDefault(timeZone);

        // act
        List<String> result = new ArrayList<>();
        while (resultSet.next()) {
            Time timestamp = resultSet.getTime("time", Calendar.getInstance(timeZone));
            result.add(timestamp.toString());
        }

        // assert
        assertThat(result, contains(
                is("01:01:11"),
                is("05:05:11"),
                is("09:09:11"),
                is("12:12:30"),
                is("16:16:40"),
                is("23:23:50")
        ));
    }

    private void prepareResultSet(List<Field> fields, List<Tuple> tuples) {
        Field[] fieldsArray = fields.toArray(new Field[0]);
        resultSet = new DtmResultSet(connection, statement, fieldsArray, tuples);
    }
}