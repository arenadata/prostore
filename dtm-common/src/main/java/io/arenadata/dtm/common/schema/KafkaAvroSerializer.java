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
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * {@link Serializer} implementation that converts byte arrays to {@link org.apache.avro.generic.GenericData.Record} objects.
 * The following configuration is needed<ul>
 * <li>{@code schemaProviderFactory=<factory_class_name>} for schema discovery</li>
 * <li>{@code schemaversion.<schema_name>=<schema_version>} for reader schema versions</li>
 * </ul>
 */
public class KafkaAvroSerializer<T extends GenericContainer> implements Serializer<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAvroSerializer.class);
    private SchemaProvider schemaProvider;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        schemaProvider = SchemaUtils.getSchemaProvider(configs);
    }

    @Override
    public byte[] serialize(String topic, T data) {
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            VersionedSchema schema = getSchema(data, topic);

            writeSchemaId(stream, schema.getId());
            writeSerializedAvro(stream, data, schema.getSchema());

            return stream.toByteArray();
        } catch (Exception e) {
            LOGGER.error("Serialization error", e);
            throw new RuntimeException("Could not serialize data", e);
        }
    }

    private void writeSchemaId(ByteArrayOutputStream stream, int id) throws IOException {
        try (DataOutputStream os = new DataOutputStream(stream)) {
            os.writeInt(id);
        }
    }

    private void writeSerializedAvro(ByteArrayOutputStream stream, T data, Schema schema) throws IOException {
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
        DatumWriter<T> datumWriter = new GenericDatumWriter<>(schema);
        datumWriter.write(data, encoder);
        encoder.flush();
    }

    private VersionedSchema getSchema(T data, String topic) {
        return schemaProvider.getMetadata(data.getSchema());
    }

    @Override
    public void close() {
        try {
            schemaProvider.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
