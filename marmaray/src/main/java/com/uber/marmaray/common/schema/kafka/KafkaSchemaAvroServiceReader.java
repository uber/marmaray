package com.uber.marmaray.common.schema.kafka;

import com.uber.marmaray.common.exceptions.InvalidDataException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;

public class KafkaSchemaAvroServiceReader extends AbstractKafkaSchemaServiceReader {
    public KafkaSchemaAvroServiceReader(Schema schema) {
        super(schema);
    }

    @Override
    public GenericRecord read(byte[] buffer) throws InvalidDataException {
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(getSchema());
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(buffer, null);
        try {
            return datumReader.read(null, decoder);
        } catch (IOException e) {
            throw new InvalidDataException("Error decoding data", e);
        }
    }
}
