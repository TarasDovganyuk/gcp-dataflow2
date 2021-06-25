package com.epam.function;

import com.epam.entity.DataPrepContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.JSONObject;
import org.yaml.snakeyaml.Yaml;

import java.util.Arrays;
import java.util.Map;

@Slf4j
public class DataPrepContextFn extends DoFn<String, DataPrepContextFn> {

    private static String convertToJson(String yamlString) {
        Yaml yaml = new Yaml();
        Map<String, Object> map = yaml.load(yamlString);

        JSONObject jsonObject = new JSONObject(map);
        return jsonObject.toString();
    }

    @ProcessElement
    public void procesElement(@Element String element, OutputReceiver<DataPrepContextFn> receiver) {
        log.info(String.format("Element: %s", element));
//        DataPrepContext dataPrepContext = new DataPrepContext();
        String[] words = element.split(",");
        String yamlJson = convertToJson(element);
        log.info(String.format("Yaml Json: %s", yamlJson));
        log.info(String.format("Words: %s", Arrays.toString(words)));

//        for(int i = 1; i < words.length; i++) {
//            dataPrepContext.getFields().put()
//        }

        receiver.output(new DataPrepContextFn());

    }

}
