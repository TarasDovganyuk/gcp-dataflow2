package com.epam.entity;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class DataPrepContext {
   private Map<String, Map<String, String>> fields;
}
