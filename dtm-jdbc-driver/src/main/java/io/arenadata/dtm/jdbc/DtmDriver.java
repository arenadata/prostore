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
package io.arenadata.dtm.jdbc;

import io.arenadata.dtm.jdbc.ext.DtmConnectionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;

import static io.arenadata.dtm.jdbc.util.DriverConstants.*;
import static io.arenadata.dtm.jdbc.util.UrlConnectionParser.parseURL;

public class DtmDriver implements Driver {

    private static final Logger PARENT_LOGGER = LoggerFactory.getLogger("io.arenadata.dtm.driver.jdbc");
    private static final Logger LOGGER = LoggerFactory.getLogger("io.arenadata.dtm.driver.jdbc.DtmDriver");

    static {
        try {
            DriverManager.registerDriver(new DtmDriver());
            LOGGER.info("Driver registered");
        } catch (SQLException e) {
            LOGGER.error("Error registering JDBC driver " + e.getCause());
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        parseURL(url, info);

        return makeConnection(url, info);
    }

    private static Connection makeConnection(String url, Properties info) throws SQLException {
        return new DtmConnectionImpl(dbHost(info), user(info), schema(info), info, url);
    }

    private static String schema(Properties info) {
        return info.getProperty(SCHEMA_PROPERTY, "");
    }

    private static String user(Properties info) {
        return info.getProperty(USER_PROPERTY, "");
    }

    private static String dbHost(Properties info) {
        return info.getProperty(HOST_PROPERTY, "");
    }

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith(CONNECT_URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
        return (java.util.logging.Logger) PARENT_LOGGER;
    }
}
