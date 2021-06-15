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
package io.arenadata.dtm.jdbc.util;

public class DriverInfo {
    public static final String DATABASE_PRODUCT_NAME = "DTM";
    public static final String DRIVER_NAME = "DTM JDBC Driver";
    public static final String DRIVER_SHORT_NAME = "DtmJDBC";
    public static final String DRIVER_VERSION = "3.7.2";
    public static final String DRIVER_FULL_NAME = "DTM JDBC Driver 3.7.2";
    public static final int MAJOR_VERSION = 3;
    public static final int MINOR_VERSION = 7;
    public static final int PATCH_VERSION = 2;
    public static final String JDBC_VERSION = "4.2";
    public static final int JDBC_MAJOR_VERSION = "4.2".charAt(0) - 48;
    public static final int JDBC_MINOR_VERSION = "4.2".charAt(2) - 48;

    private DriverInfo() {
    }
}
