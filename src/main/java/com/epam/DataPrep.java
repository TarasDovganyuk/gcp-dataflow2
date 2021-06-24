package com.epam;

import com.epam.common.DataPrepOptions;
import com.epam.function.ConvertCsvToAvro;
import com.epam.function.ParseCsvFn;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.json.JSONObject;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DataPrep {

    private static String convertToJson(String yamlString) {
        Yaml yaml = new Yaml();
        Map<String, Object> map = yaml.load(yamlString);

//        Map<String, LinkedHashMap<String, LinkedHashMap<String, String>>> map = yaml.load(yamlString);

        log.info(String.format("LINKED HASH MAP: %s", map));
//        log.info(String.format("LINKED HASH MAP KEYSET: %s", map.keySet()));
//        log.info(String.format("LINKED HASH MAP VALUES: %s", map.values()));
//        log.info(String.format("INSIDE MAP: %s", map.get("layouts").get("USER").keySet()));

//        log.info(String.format("User name: %s", map.get("USER")));


        JSONObject jsonObject = new JSONObject(map);

        return jsonObject.toString();
    }

    private static String convertYamlToJson(String yaml) {
        try {
            ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
            Yaml obj = yamlReader.readValue(yaml, Yaml.class);
            ObjectMapper jsonWriter = new ObjectMapper();
            return jsonWriter.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }


    public static String getSchema(String schemaPath) throws IOException {
        ReadableByteChannel chan = FileSystems.open(FileSystems.matchNewResource(
                schemaPath, false));

        try (InputStream stream = Channels.newInputStream(chan)) {
            BufferedReader streamReader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
            StringBuilder dataBuilder = new StringBuilder();

            String line;
            while ((line = streamReader.readLine()) != null) {
                dataBuilder.append(line);
            }

            return dataBuilder.toString();
        }
    }

    static void runDataPrep(DataPrepOptions options) throws IOException {
        Pipeline pipeline = Pipeline.create(options);



        String jsonFromYaml = convertToJson(new String(Files.readAllBytes(Paths.get(
                "src/main/resources/profile.yaml"))));


        log.info(String.format("Json from Yaml: %s", jsonFromYaml));

        Map<String, Object> result =
                new ObjectMapper().readValue(jsonFromYaml, HashMap.class);

//        log.info(String.format("Map: %s", result.get("USER").getClass()));
//        LinkedHashMap linkedHashMap = (LinkedHashMap) result.get("USER");
//        log.info(String.format("Map values: %s", linkedHashMap.values()));


        pipeline
                .apply("Read configuration", TextIO.read().from("src/main/resources/profile.yaml"))
                .apply("Parse configuration", ParDo.of(new ParseCsvFn()));
//                .apply("Read lines", TextIO.read().from(options.getInputFiles()))
//                .apply("Parse csv", ParDo.of(new ParseCsvFn()));
//                .apply("Transform Csv to Avro", ParDo.of(new ConvertCsvToAvro(getSchema("src/main/resources/schema.json"))))
//                .setCoder(AvroCoder.of(GenericRecord.class, new Schema.Parser().parse(getSchema("src/main/resources/schema.json"))))
//                .apply("Write Avro formatted data", AvroIO.writeGenericRecords(getSchema("src/main/resources/schema.json"))
//                        .to(options.getOutput()).withCodec(CodecFactory.snappyCodec()).withSuffix(".avro"));


        pipeline.run().waitUntilFinish();
    }

    public static void main(String[] args) throws IOException {
        DataPrepOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataPrepOptions.class);
        runDataPrep(options);
    }
}
