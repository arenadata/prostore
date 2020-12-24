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

import org.apache.avro.Schema;

public class VersionedSchema {
    private final int id;
    private final String name;
    private final int version;
    private final Schema schema;

    public VersionedSchema(int id, String name, int version, Schema schema) {
        this.id = id;
        this.name = name;
        this.version = version;
        this.schema = schema;
    }

    public String getName() {
        return name;
    }

    public int getVersion() {
        return version;
    }

    public Schema getSchema() {
        return schema;
    }


    public int getId() {
        return id;
    }


}
