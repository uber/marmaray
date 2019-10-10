package com.uber.marmaray.common.schema.kafka;

import com.uber.marmaray.common.exceptions.InvalidDataException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.JsonDecoder;

import java.io.IOException;

public class KafkaSchemaJSONServiceReader extends AbstractKafkaSchemaServiceReader {
    public KafkaSchemaJSONServiceReader(Schema schema) {
        super(schema);
    }

    @Override
    public GenericRecord read(byte[] buffer) throws InvalidDataException {
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(getSchema());
        try {
            JsonDecoder decoder = DecoderFactory.get().jsonDecoder(getSchema(), new String(buffer));
            return datumReader.read(null, decoder);
        } catch (IOException e) {
            throw new InvalidDataException("Error decoding data", e);
        }
    }
}
