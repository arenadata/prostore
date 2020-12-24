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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * {@link Deserializer} implementation that converts byte arrays to {@link org.apache.avro.generic.GenericData.Record} objects.
 * The following configuration is needed<ul>
 * <li>{@code schemaProviderFactory=<factory_class_name>} for schema discovery</li>
 * <li>{@code schemaversion.<schema_name>=<schema_version>} for reader schema versions</li>
 * </ul>
 */
public class KafkaAvroRecordDeserializer implements Deserializer<GenericData.Record> {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAvroRecordDeserializer.class);
    private Map<String, VersionedSchema> readerSchemasByName;
    private SchemaProvider schemaProvider;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        schemaProvider = SchemaUtils.getSchemaProvider(configs);
        readerSchemasByName = SchemaUtils.getVersionedSchemas(configs, schemaProvider);
    }


    @Override
    public GenericData.Record deserialize(String topic, byte[] data) {
        try (ByteArrayInputStream stream = new ByteArrayInputStream(data)) {
            //В первом байте идентификатор схемы,для получения тела схемы из schema registry
            int schemaId = SchemaUtils.readSchemaId(stream);
            VersionedSchema writerSchema = schemaProvider.get(schemaId);
            if (writerSchema == null) {
                throw new RuntimeException("Record scheme with id not defined=" + schemaId);
            }

            VersionedSchema readerSchema = readerSchemasByName.get(writerSchema.getName());
            if (readerSchema == null) {
                throw new RuntimeException("No reader defined with name" + writerSchema.getName());
            }
            GenericData.Record avroRecord = readAvroRecord(stream, writerSchema.getSchema(), readerSchema.getSchema());
            return avroRecord;
        } catch (Exception e) {
            LOGGER.error("Deserialization error", e);
            throw new RuntimeException(e);
        }
    }

    private GenericData.Record readAvroRecord(InputStream stream, Schema writerSchema, Schema readerSchema) throws IOException {
        DatumReader<Object> datumReader = new GenericDatumReader<>(writerSchema, readerSchema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(stream, null);
        GenericData.Record record = new GenericData.Record(readerSchema);
        datumReader.read(record, decoder);
        return record;
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
