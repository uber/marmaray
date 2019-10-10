package com.uber.marmaray.common.schema.kafka;


import com.uber.marmaray.common.schema.ISchemaService;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.hibernate.validator.constraints.NotEmpty;

import java.io.Serializable;

@Slf4j
public abstract class AbstractKafkaSchemaServiceReader implements ISchemaService.ISchemaServiceReader, Serializable {
    private final String schemaString;
    private transient Schema schema;

    AbstractKafkaSchemaServiceReader(@NotEmpty final Schema schema) {
        this.schemaString = schema.toString();
        this.schema = schema;
        log.info("Kafka Schema service reader initialised with schema {}", schemaString);
    }

    Schema getSchema() {
        if (this.schema == null) {
            this.schema = new Schema.Parser().parse(this.schemaString);
        }
        return this.schema;
    }
}