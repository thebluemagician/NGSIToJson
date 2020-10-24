package org.apache.nifi.processors.ngsi2json.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.nifi.processors.ngsi2json.util.Constants.*;

/**
 * Processes and maps the ngsi flow file to json data based on provided properties attributes.
 * @author Md.Adil
 *
 */
public class FlowFileMappper {

  Logger logger = LoggerFactory.getLogger(FlowFileMappper.class);

  /**
   * Base attribute mapper method.
   * 
   * @param ngsiVersion
   * @param jsonStructure
   * @param metaDataFlag
   * @param flowFileContent
   * @param flowFileAttributes
   * @return processedFlowFile
   */
  public String attributeMapper(String ngsiVersion, String jsonStructure, Boolean metaDataFlag,
      String flowFileContent, Map<String, String> flowFileAttributes) {

    String processedData = "";
    String attrsFormat = flowFileAttributes.get(NGSIv2_ATTRSFORMAT);

    if (ATTRFORMAT_NORMALIZED.equalsIgnoreCase(attrsFormat)) {

      /* processing data for normalized ngsi attributes */
      processedData = normalizedAttrMapper(jsonStructure, metaDataFlag, flowFileContent);

    } else if (ATTRFORMAT_KEYVALUE.equalsIgnoreCase(attrsFormat)) {

      /* processing data for keyValues ngsi attributes */
      processedData = keyValueAttrMapper(jsonStructure, metaDataFlag, flowFileContent);

    }

    return processedData;
  }

/**
 * This handles NGSI Flowfile having attrFormat:keyValues.
 * 
 * @param jsonStructure
 * @param metaDataFlag
 * @param flowFileContent
 * @return processedKeyValueFlow
 */
  public String keyValueAttrMapper(String jsonStructure, Boolean metaDataFlag,
      String flowFileContent) {

    JSONObject jsonFlowFile = toJson(flowFileContent);
    JSONObject jsonData = new JSONObject();
    JSONObject jsonMetaData = new JSONObject();
    ArrayList<String> metaAttrs = new ArrayList<String>(
        new ArrayList<String>(Arrays.asList(SUBSCRIPTION_ID, TYPE, ID, TIMEINSTANT)));

    for (String key : jsonFlowFile.keySet()) {
      if (key.equals(DATA)) {
        JSONObject dataObject = jsonFlowFile.getJSONArray(key).getJSONObject(0);

        for (String jsonKey : dataObject.keySet()) {
          if (metaAttrs.contains(jsonKey)) {
            jsonMetaData.put(jsonKey, dataObject.get(jsonKey));
          } else {
            jsonData.put(jsonKey, dataObject.get(jsonKey));
          }
        }
      } else if (metaAttrs.contains(key)) {
        jsonMetaData.put(key, jsonFlowFile.get(key));
      }
    }

    JSONObject processedData =
        jsonStructureMetaData(jsonData, jsonMetaData, metaDataFlag, jsonStructure);

    return processedData.toString();
  }

  /**
   * This handles NGSI Flowfile having attrFormat:normalized.
   * 
   * @param jsonStructure
   * @param metaDataFlag
   * @param flowFileContent
   * @return proccessedNormalizedAttr
   */
  public String normalizedAttrMapper(String jsonStructure, Boolean metaDataFlag,
      String flowFileContent) {

    JSONObject jsonFlowFile = toJson(flowFileContent);
    JSONObject jsonData = new JSONObject();
    JSONObject jsonMetaData = new JSONObject();
    ArrayList<String> metaAttrs = new ArrayList<String>(
        new ArrayList<String>(Arrays.asList(SUBSCRIPTION_ID, TYPE, ID, TIMEINSTANT)));

    for (String key : jsonFlowFile.keySet()) {
      if (key.equals(DATA)) {
        JSONObject dataObject = jsonFlowFile.getJSONArray(key).getJSONObject(0);

        for (String jsonKey : dataObject.keySet()) {
          if (metaAttrs.contains(jsonKey)) {
            if (jsonKey.equals(TIMEINSTANT)) {
              jsonMetaData.put(jsonKey, dataObject.getJSONObject(jsonKey).getString(VALUE));
            } else {
              jsonMetaData.put(jsonKey, dataObject.get(jsonKey));
            }
          } else {
            jsonData.put(jsonKey, dataObject.getJSONObject(jsonKey).get(VALUE));
          }
        }
      } else if (metaAttrs.contains(key)) {
        jsonMetaData.put(key, jsonFlowFile.get(key));
      }
    }

    JSONObject processedData =
        jsonStructureMetaData(jsonData, jsonMetaData, metaDataFlag, jsonStructure);

    return processedData.toString();
  }

  /**
   * Converts the JsonString into JsonObjects.
   * 
   * @param jsonString
   * @return JsonObject
   */
  JSONObject toJson(String jsonString) {

    JSONObject element = new JSONObject(jsonString);
    return element;
  }

  /**
   * Creates the processed data json structure according with the provided metadata and other
   * provided details through properties.
   * 
   * @param jsonData
   * @param jsonMetaData
   * @param metaDataFlag
   * @param jsonStructure
   * @return JSONObject
   */
  JSONObject jsonStructureMetaData(JSONObject jsonData, JSONObject jsonMetaData,
      Boolean metaDataFlag, String jsonStructure) {

    JSONObject finalData = new JSONObject();

    if (FLAT.equalsIgnoreCase(jsonStructure)) {

      if (metaDataFlag == Boolean.FALSE) {
        finalData = jsonData;
      } else if (metaDataFlag == Boolean.TRUE) {
        finalData = jsonData;
        for (String key : jsonMetaData.keySet()) {
          finalData.put(key, jsonMetaData.get(key));
        }
      }
    } else if (NESTED.equalsIgnoreCase(jsonStructure)) {

      if (metaDataFlag == Boolean.FALSE) {
        finalData.put(DATA, jsonData).put(METADATA, new JSONObject());
      } else if (metaDataFlag == Boolean.TRUE) {
        finalData.put(DATA, jsonData).put(METADATA, jsonMetaData);
      }
    } else {
      return jsonData;
    }

    return finalData;
  }
}
