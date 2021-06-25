package com.epam;

import com.epam.common.DataPrepOptions;
import com.epam.entity.DataPrepContext;
import com.epam.function.ParseCsvFn;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.json.JSONObject;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

@Slf4j
public class DataPrep {

    private static String convertToJson(String yamlString) {
        Yaml yaml = new Yaml();
        Map<String, Object> map = yaml.load(yamlString);
//        Map<String, LinkedHashMap<String, String>> fields = (LinkedHashMap) map.get("layouts");


//        Map<String, LinkedHashMap<String, LinkedHashMap<String, String>>> map = yaml.load(yamlString);

        log.info(String.format("LINKED HASH MAP: %s", map));
//        log.info(String.format("LINKED HASH MAP KEYSET: %s", map.keySet()));
//        log.info(String.format("LINKED HASH MAP VALUES: %s", map.values()));
//        log.info(String.format("INSIDE MAP: %s", map.get("layouts").get("USER").keySet()));

//        log.info(String.format("User name: %s", map.get("USER")));


        JSONObject jsonObject = new JSONObject(map);

        return jsonObject.toString();
    }

    private static String convertToMap(String yamlString) {
        Yaml yaml = new Yaml();
        Map<String, Object> map = yaml.load(yamlString);
//        Map<String, LinkedHashMap<String, String>> fields = (LinkedHashMap) map.get("layouts");


//        Map<String, LinkedHashMap<String, LinkedHashMap<String, String>>> map = yaml.load(yamlString);

        log.info(String.format("LINKED HASH MAP: %s", map));
//        log.info(String.format("LINKED HASH MAP KEYSET: %s", map.keySet()));
//        log.info(String.format("LINKED HASH MAP VALUES: %s", map.values()));
//        log.info(String.format("INSIDE MAP: %s", map.get("layouts").get("USER").keySet()));

//        log.info(String.format("User name: %s", map.get("USER")));


        JSONObject jsonObject = new JSONObject(map);

        return jsonObject.toString();
    }

    static void runDataPrep(DataPrepOptions options) throws IOException {
        Pipeline pipeline = Pipeline.create(options);

        Yaml yaml = new Yaml();
        Map<String, Object> map = yaml.load(new String(Files.readAllBytes(Paths.get(
                options.getProfile()))));
        DataPrepContext.layouts = (LinkedHashMap) map.get("layouts");
        DataPrepContext.actions = (ArrayList) map.get("actions");

        log.info(String.format("map: %s", map));
        log.info(String.format("layouts: %s", DataPrepContext.layouts));
        log.info(String.format("actions: %s", DataPrepContext.actions));
        log.info(String.format("USER: %s", DataPrepContext.layouts.get("USER")));


//        Yaml yaml = new Yaml();
//        Map<String, Object> map = yaml.load(new String(Files.readAllBytes(Paths.get(
//                "src/main/resources/profile.yaml"))));
//
//        DataPrepContext dataPrepContext = new DataPrepContext((LinkedHashMap) map.get("layouts"));



//        log.info(String.format("Map: %s", map));
//        log.info(String.format("Layouts: %s", dataPrepContext.getFields()));

//        String jsonFromYaml = convertToJson(new String(Files.readAllBytes(Paths.get(
//                "src/main/resources/profile.yaml"))));


//        log.info(String.format("Json from Yaml: %s", jsonFromYaml));

//        Map<String, Object> result =
//                new ObjectMapper().readValue(jsonFromYaml, HashMap.class);

//        log.info(String.format("Map: %s", result.get("USER").getClass()));
//        LinkedHashMap linkedHashMap = (LinkedHashMap) result.get("USER");
//        log.info(String.format("Map values: %s", linkedHashMap.values()))
//        ;


        pipeline.apply("Read csv file", TextIO.read().from(options.getInputFiles()))
                .apply("Parse csf file", ParDo.of(new ParseCsvFn()))
//        .apply("Transform to output", )
        ;
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
