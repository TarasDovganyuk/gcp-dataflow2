package com.epam.function;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.List;

@Slf4j
public class ConvertCsvToAvro extends DoFn<String, GenericRecord> {
    private String schemaJson;

    public ConvertCsvToAvro(String schemaJson) {
        this.schemaJson = schemaJson;
    }



    @ProcessElement
    public void processElement(ProcessContext ctx) throws IllegalArgumentException, IOException {
        // Split CSV row into using delimiter
        String[] rowValues = ctx.element().split(",");

        Schema schema = new Schema.Parser().parse(schemaJson);

        // Create Avro Generic Record
        GenericRecord genericRecord = new GenericData.Record(schema);
        List<Schema.Field> fields = schema.getFields();

        for (int index = 0; index < fields.size(); ++index) {
            Schema.Field field = fields.get(index);
            String fieldType = field.schema().getType().getName().toLowerCase();

            switch (fieldType) {
                case "string":
                    genericRecord.put(field.name(), rowValues[index]);
                    break;
                case "boolean":
                    genericRecord.put(field.name(), Boolean.valueOf(rowValues[index]));
                    break;
                case "int":
                    genericRecord.put(field.name(), Integer.valueOf(rowValues[index]));
                    break;
                case "long":
                    genericRecord.put(field.name(), Long.valueOf(rowValues[index]));
                    break;
                case "float":
                    genericRecord.put(field.name(), Float.valueOf(rowValues[index]));
                    break;
                case "double":
                    genericRecord.put(field.name(), Double.valueOf(rowValues[index]));
                    break;
                default:
                    log.error("Data transformation doesn't support: " + fieldType);
                    throw new IllegalArgumentException("Field type " + fieldType + " is not supported.");
            }
        }
        ctx.output(genericRecord);
    }

}
