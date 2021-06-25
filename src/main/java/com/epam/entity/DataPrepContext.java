package com.epam.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Data
@AllArgsConstructor
public class DataPrepContext {
   public static Map<String, LinkedHashMap<String, String>> layouts;
   public static ArrayList<LinkedHashMap<String, String>> actions;
}
