package org.apache.nifi.processors.ngsi2json.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.map.CaseInsensitiveMap;
import org.apache.nifi.components.PropertyDescriptor;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowFileMappper {

  Logger logger = LoggerFactory.getLogger(FlowFileMappper.class);

  public JSONObject map2json(Map<PropertyDescriptor, String> processorProperties,
      String ngsiFlowFile, Map<String, String> flowFileAttributes) {


    Map<String, String> newFlowFileAttributes =
        new CaseInsensitiveMap<String, String>(flowFileAttributes);

    JSONObject test2 = new JSONObject();

    for (Map.Entry<String, String> entry : newFlowFileAttributes.entrySet()) {
      test2.put(entry.getKey(), entry.getValue());
    }

    logger.error(ngsiFlowFile);
    /*
     * JsonReader jsonReader = new JsonReader(new StringReader(ngsiFlowFile));
     * jsonReader.setLenient(true); JsonElement element = JsonParser.parseReader(jsonReader);
     */

    JSONObject element = new JSONObject(ngsiFlowFile);
    test2.put("ngsiData", element);
    return test2;
  }

  // public JSONObject attributeMapper(Map<PropertyDescriptor, String> processorProperties,
  // String ngsiFlowFile, Map<String, String> flowFileAttributes) {
  //
  // Map<String, String> generatedAttributes = new HashMap<String, String>();
  //
  // for (final Map.Entry<PropertyDescriptor, String> entry : processorProperties.entrySet()) {
  // PropertyDescriptor property = entry.getKey();
  // if (property.isDynamic() && property.isExpressionLanguageSupported()) {
  // String dynamicValue = processorProperties.get(property);
  // generatedAttributes.put(property.getName(), dynamicValue);
  // }
  // }
  //
  // JSONObject test2 = new JSONObject();
  //
  // for (Map.Entry<String, String> entry : generatedAttributes.entrySet()) {
  // test2.put(entry.getKey(), entry.getValue());
  // }
  //
  // return test2;
  // }

  public String attributeMapper(String attribute, String ngsiVersion, Boolean metaDataFlag,
      String flowFileContent, Map<String, String> flowFileAttributes) {

    JSONObject jsonFlowFile = toJson(flowFileContent);
    JSONObject jsonData = new JSONObject();
    JSONObject jsonMetaData = new JSONObject();
    ArrayList<String> metaAttrs = new ArrayList<String>(
        new ArrayList<String>(Arrays.asList("subscriptionId", "type", "id", "TimeInstant")));
    
    logger.error("JsonFlowFile: " + jsonFlowFile.toString());


    for (String key : jsonFlowFile.keySet()) {
      if (key.equals("data")) {
        JSONObject dataObject = jsonFlowFile.getJSONArray(key).getJSONObject(0);

        for (String jsonKey : dataObject.keySet()) {
          if (metaAttrs.contains(jsonKey)) {
            if (jsonKey.equals("TimeInstant")) {
              jsonMetaData.put(jsonKey, dataObject.getJSONObject(jsonKey).getString("value"));
            } else {
              jsonMetaData.put(jsonKey, dataObject.get(jsonKey));
            }
          } else {
            jsonData.put(jsonKey, dataObject.getJSONObject(jsonKey).get("value"));
          }
        }
      } else if (metaAttrs.contains(key)) {
        jsonMetaData.put(key, jsonFlowFile.get(key));
      }
    }

    logger.error("JSON_DATA: " + jsonData.toString());
    JSONObject processedData = new JSONObject();

    if (metaDataFlag == Boolean.FALSE) {
      processedData = jsonData;
    } else if (metaDataFlag == Boolean.TRUE) {
      processedData.put("data", jsonData).put("metadata", jsonMetaData).toString();
    }

    logger.error("processed data: " + processedData.toString());
    return processedData.toString();
  }

  JSONObject toJson(String jsonString) {

    JSONObject element = new JSONObject(jsonString);
    return element;
  }
  
  JSONObject jsonStructure() {
    
    return null;
  }
}
