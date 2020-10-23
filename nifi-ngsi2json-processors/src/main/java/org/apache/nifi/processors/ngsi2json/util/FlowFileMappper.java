package org.apache.nifi.processors.ngsi2json.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.nifi.processors.ngsi2json.util.Constants.*;

public class FlowFileMappper {

  Logger logger = LoggerFactory.getLogger(FlowFileMappper.class);

  public String attributeMapper(String attribute, String ngsiVersion, String jsonStructure,
      Boolean metaDataFlag, String flowFileContent, Map<String, String> flowFileAttributes) {

    String processedData = "";
    String attrsFormat = flowFileAttributes.getOrDefault(NGSIv2_ATTRSFORMAT, null);

    if (ATTRFORMAT_NORMALIZED.equalsIgnoreCase(attrsFormat)) {

      processedData = normalizedAttrMapper(attribute, ngsiVersion, jsonStructure, metaDataFlag,
          flowFileContent, flowFileAttributes);
    } else if (ATTRFORMAT_KEYVALUE.equalsIgnoreCase(attrsFormat)) {
      processedData = keyValueAttrMapper(attribute, ngsiVersion, jsonStructure, metaDataFlag,
          flowFileContent, flowFileAttributes);
    } else {
      return null;
    }

    return processedData;
  }

  public String keyValueAttrMapper(String attribute, String ngsiVersion, String jsonStructure,
      Boolean metaDataFlag, String flowFileContent, Map<String, String> flowFileAttributes) {

    JSONObject jsonFlowFile = toJson(flowFileContent);
    JSONObject jsonData = new JSONObject();
    JSONObject jsonMetaData = new JSONObject();
    ArrayList<String> metaAttrs = new ArrayList<String>(
        new ArrayList<String>(Arrays.asList(SUBSCRIPTION_ID, TYPE, ID, TIMEINSTANT)));

    for (String key : jsonFlowFile.keySet()) {
      if (key.equals("data")) {
        JSONObject dataObject = jsonFlowFile.getJSONArray(key).getJSONObject(0);

        for (String jsonKey : dataObject.keySet()) {
          if (metaAttrs.contains(jsonKey)) {
            if (jsonKey.equals("TimeInstant")) {
              jsonMetaData.put(jsonKey, dataObject.get(jsonKey));
            } else {
              jsonMetaData.put(jsonKey, dataObject.get(jsonKey));
            }
          } else {
            jsonData.put(jsonKey, dataObject.get(jsonKey));
          }
        }
      } else if (metaAttrs.contains(key)) {
        jsonMetaData.put(key, jsonFlowFile.get(key));
      }
    }
    /*
     * for (final Map.Entry<PropertyDescriptor, String> entry : processorProperties.entrySet()) {
     * PropertyDescriptor property = entry.getKey(); if (property.isDynamic() &&
     * property.isExpressionLanguageSupported()) { String dynamicValue =
     * processorProperties.get(property); generatedAttributes.put(property.getName(), dynamicValue);
     * } }
     */

    JSONObject processedData = new JSONObject();

    if (metaDataFlag == Boolean.FALSE) {
      processedData = jsonData;
    } else if (metaDataFlag == Boolean.TRUE) {
      processedData.put("data", jsonData).put("metadata", jsonMetaData).toString();
    }

    logger.error("processed data: " + processedData.toString());
    return processedData.toString();
  }

  public String normalizedAttrMapper(String attribute, String ngsiVersion, String jsonStructure,
      Boolean metaDataFlag, String flowFileContent, Map<String, String> flowFileAttributes) {

    JSONObject jsonFlowFile = toJson(flowFileContent);
    JSONObject jsonData = new JSONObject();
    JSONObject jsonMetaData = new JSONObject();
    ArrayList<String> metaAttrs = new ArrayList<String>(
        new ArrayList<String>(Arrays.asList(SUBSCRIPTION_ID, TYPE, ID, TIMEINSTANT)));

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
