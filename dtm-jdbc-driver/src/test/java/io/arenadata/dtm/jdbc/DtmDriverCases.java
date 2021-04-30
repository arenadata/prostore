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
package io.arenadata.dtm.jdbc;

import io.arenadata.dtm.jdbc.core.BaseConnection;
import io.arenadata.dtm.jdbc.ext.DtmConnectionImpl;
import io.arenadata.dtm.jdbc.ext.DtmPreparedStatement;
import io.arenadata.dtm.jdbc.ext.DtmResultSet;
import io.arenadata.dtm.jdbc.ext.DtmStatement;

import java.sql.*;

public class DtmDriverCases {

    public static void main(String[] args) throws SQLException {
        String host = "localhost:9090";
        String user = "";
        String schema = "";
        String url = String.format("jdbc:adtm://%s/", host);
        BaseConnection conn = new DtmConnectionImpl(host, user, schema, null, url);
        DtmStatement stmnt = (DtmStatement) conn.createStatement();
        DatabaseMetaData metaData = conn.getMetaData();
        //ResultSet resultSet = stmnt.executeQuery("use dtm_1012");
        //ResultSet resultSet = metaData.getSchemas();
        //ResultSet resultSet = metaData.getColumns("dtm_1012", "", "accounts_all", null);
        //final ResultSet resultSet = testPrepareStmnt(conn);
        final ResultSet resultSet = testStmnt(conn);
        //Time t9 = (Time) resultSet.getObject(10);
        //Time t10 = (Time) resultSet.getObject(11);
        Timestamp t5 = (Timestamp) resultSet.getObject(5);
        Timestamp t6 = (Timestamp) resultSet.getObject(6);
        //Timestamp t12 = (Timestamp) resultSet.getObject(13);
        //resultSet.findColumn("TABLE_CAT");
        System.out.println(resultSet);
    }

    private static ResultSet testStmnt(BaseConnection conn) throws SQLException {
        String sql = "select t1.* from dtm_1046.accounts t1 datasource_type='ADB'";
        DtmStatement stmnt = (DtmStatement) conn.createStatement();
        return stmnt.executeQuery(sql);
    }

    private static ResultSet testPrepareStmnt(BaseConnection conn) throws SQLException {
        final String sql = "select * from dtm_928_2.all_types_table " +
                "where id = ? " +
                " and double_col = ?" +
                " and float_col = ?" +
                " and varchar_col = ?" +
                " and boolean_col = ?" +
                " and int_col = ?" +
                " and bigint_col = ?" +
                " and date_col = ?" +
                " and timestamp_col = ?" +
                " and time_col = ?" +
                " and uuid_col = ?" +
                " and char_col = ?" +
                //" datasource_type='adg'" +
                "";

        DtmPreparedStatement stmnt = (DtmPreparedStatement) conn.prepareStatement(sql);
        stmnt.setInt(0, 1);
        stmnt.setDouble(1, 1d);
        stmnt.setFloat(2, 1.0f);
        stmnt.setString(3, "sss");
        stmnt.setBoolean(4, true);
        stmnt.setInt(5, 1);
        stmnt.setLong(6, 100000L);
        stmnt.setDate(7, Date.valueOf("6365-01-31"));
        stmnt.setTimestamp(8,  Timestamp.valueOf("2020-11-17 21:11:12"));
        stmnt.setTime(9, Time.valueOf("00:01:40"));
        stmnt.setString(10, "d92beee8-749f-4539-aa15-3d2941dbb0f1");
        stmnt.setString(11, "c");
        final ResultSet resultSet = stmnt.executeQuery();
        return resultSet;
    }
}
