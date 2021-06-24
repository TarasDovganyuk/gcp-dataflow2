package com.epam.function;

import com.epam.entity.Csv;
import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.JSONObject;
import org.yaml.snakeyaml.Yaml;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
public class ParseCsvFn extends DoFn<String, /*Csv*/String> {

    @ProcessElement
    public void procesElement(@Element String element, OutputReceiver</*Csv*/String> receiver) {
//      log.info(String.format("Element: %s", element));
      String[] words = element.split(",");
//      log.info(String.format("Words: %s", Arrays.toString(words)));
      receiver.output(element);

    }

}
