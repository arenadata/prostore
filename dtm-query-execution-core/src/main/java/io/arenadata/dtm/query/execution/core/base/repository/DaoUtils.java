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
package io.arenadata.dtm.query.execution.core.base.repository;

import com.fasterxml.jackson.databind.MapperFeature;
import io.arenadata.dtm.common.configuration.core.CoreConstants;
import io.arenadata.dtm.common.exception.DtmException;
import io.vertx.core.json.jackson.DatabindCodec;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public final class DaoUtils {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private DaoUtils() {
    }

    public static <T> T deserialize(byte[] bytes, String datamart, Class<T> clazz) {
        try {
            return DatabindCodec.mapper().readValue(bytes, clazz);
        } catch (Exception e) {
            throw new DtmException(String.format("Can't deserialize %s [%s]", clazz.getName(), datamart), e);
        }
    }

    public static <T> T deserialize(byte[] bytes, String datamart, Class<T> clazz, MapperFeature... features) {
        try {
            return DatabindCodec.mapper().enable(features).readValue(bytes, clazz);
        } catch (Exception e) {
            throw new DtmException(String.format("Can't deserialize %s [%s]", clazz.getName(), datamart), e);
        }
    }

    public static byte[] serialize(Object obj) {
        try {
            return DatabindCodec.mapper().writeValueAsBytes(obj);
        } catch (Exception e) {
            throw new DtmException(String.format("Can't serialize [%s]", obj), e);
        }
    }

    public static String getCurrentDateTime() {
        return LocalDateTime.now(CoreConstants.CORE_ZONE_ID).format(DATE_TIME_FORMATTER);
    }
}
