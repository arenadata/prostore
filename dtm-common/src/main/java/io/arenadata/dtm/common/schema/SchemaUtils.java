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
package io.arenadata.dtm.common.schema;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class SchemaUtils {


    public static final String SCHEMA_PROVIDER_FACTORY_CONFIG = "schemaProviderFactory";

    public static SchemaProvider getSchemaProvider(Map<String, ?> configs) {
        String schemaProviderFactoryClassName = (String) configs.get(SCHEMA_PROVIDER_FACTORY_CONFIG);
        try {
            return ((SchemaProviderFactory)Class.forName(schemaProviderFactoryClassName).newInstance()).getProvider(configs);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, VersionedSchema> getVersionedSchemas(Map<String, ?> configs, SchemaProvider schemaProvider) {
        Map<String, VersionedSchema> schemas = new HashMap<>();
        Stream<String> schemaConfigs = configs.keySet().stream().filter(k -> k.startsWith("schemaversion."));
        schemaConfigs.forEach(k -> {
            String schemaName = k.substring("schemaversion.".length());
            Integer schemaVersion;
            if (configs.get(k) instanceof String) {
                schemaVersion = Integer.valueOf((String)configs.get(k));
            } else
            {
                schemaVersion = (Integer)configs.get(k);
            }
            VersionedSchema versionedSchema = schemaProvider.get(schemaName, schemaVersion);
            schemas.put(schemaName, versionedSchema);
        });
        return schemas;
    }

    public static int readSchemaId(InputStream stream ) throws IOException {
        try(DataInputStream is = new DataInputStream(stream)) {
            return is.readInt();
        }
    }


}
