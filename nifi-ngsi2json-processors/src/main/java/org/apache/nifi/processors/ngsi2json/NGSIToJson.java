/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.nifi.processors.ngsi2json;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.ngsi2json.util.FlowFileMappper;
import org.apache.nifi.stream.io.StreamUtils;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"Json", "NGSIv2", "NGSI", "FIWARE"})
@CapabilityDescription("Convert the content of the NGSI FlowFile to Flat or "
    + "Nested Json format. Can add property to select the fields/attribute to filter")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class NGSIToJson extends AbstractProcessor {

  private static final String JSON_STRUCTURE_FLAT = "Flat";
  private static final String JSON_STRUCTURE_NESTED = "Nested";

  public static final PropertyDescriptor JSON_TYPE = 
      new PropertyDescriptor.Builder()
        .name("Json Structure")
        .description("Specifies whether the Json should be Flat or Nested")
        .required(true).defaultValue(JSON_STRUCTURE_FLAT)
        .allowableValues(JSON_STRUCTURE_FLAT, JSON_STRUCTURE_NESTED)
        .build();

  /*
   * public static final PropertyDescriptor ATTRIBUTE = new PropertyDescriptor.Builder()
   * .name("Attributes")
   * .description("Comma separted attribute or fields to add in Json, By default all " +
   * "the data attributes along with if metadata is true, metadata attribute will be included")
   * .required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
   */
  
  public static final PropertyDescriptor NGSI_VERSION = 
      new PropertyDescriptor.Builder()
        .name("NGSI Version")
        .description("NGSIv2 is Supported")
        .required(false)
        .allowableValues("v2")
        .defaultValue("v2")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

  public static final PropertyDescriptor METADATA =
      new PropertyDescriptor.Builder()
        .name("Include Metadata")
        .description("If true, the general metadata like- id, type, subscriptionId will "
              + "be included in the FlowFile, Attributes metadata will be ignored")
        .required(true)
        .allowableValues("True", "False")
        .defaultValue("False")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

  public static final Relationship REL_SUCCESS = 
      new Relationship.Builder()
        .name("Success")
        .description("All created FlowFiles are routed to this relationship")
        .build();

  public static final Relationship REL_RETRY =
      new Relationship.Builder()
        .name("Retry")
        .description("Retry flowfile operation")
        .build();

  public static final Relationship REL_FAILURE =
      new Relationship.Builder()
        .name("Failure")
        .description("Failure of operation")
        .build();

  private List<PropertyDescriptor> descriptors;

  private Set<Relationship> relationships;

  @Override
  protected void init(final ProcessorInitializationContext context) {

    final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
    descriptors.add(JSON_TYPE);
    // descriptors.add(ATTRIBUTE);
    descriptors.add(NGSI_VERSION);
    descriptors.add(METADATA);
    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<Relationship>();
    relationships.add(REL_SUCCESS);
    relationships.add(REL_FAILURE);
    relationships.add(REL_RETRY);
    this.relationships = Collections.unmodifiableSet(relationships);
  }

  @Override
  public Set<Relationship> getRelationships() {
    return this.relationships;
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return descriptors;
  }

  @OnScheduled
  public void onScheduled(final ProcessContext context) {

  }

  String processRequest(final ProcessContext context, Map<String, String> flowFileAttributes,
      String flowFileContent) {

    /* String attribute = context.getProperty(ATTRIBUTE).getValue(); */
    String ngsiVersion = context.getProperty(NGSI_VERSION).getValue();
    String jsonStructure = context.getProperty(JSON_TYPE).getValue();
    Boolean metaDataFlag = context.getProperty(METADATA).asBoolean();

    FlowFileMappper mapper = new FlowFileMappper();

    String json = mapper.attributeMapper(ngsiVersion, jsonStructure, metaDataFlag,
        flowFileContent,flowFileAttributes);

    return json;
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session)
      throws ProcessException {

    FlowFile flowFile = session.get();
    if (flowFile == null) {
      return;
    }

    final ComponentLog logger = getLogger();
    // final AtomicReference<String> partValue = new AtomicReference<String>();
    final byte[] buffer = new byte[Math.toIntExact(flowFile.getSize())];

    session.read(flowFile, new InputStreamCallback() {
      @Override
      public void process(InputStream in) throws IOException {
        // StringWriter writer = new StringWriter();
        // IOUtils.copy(in, writer, Charset.defaultCharset());
        // partValue.set(writer.toString());

        StreamUtils.fillBuffer(in, buffer);

      }
    });

    Map<String, String> flowFileAttributes = flowFile.getAttributes();
    final String flowFileContent = new String(buffer, StandardCharsets.UTF_8);
    String value = processRequest(context, flowFileAttributes, flowFileContent);

    logger.info("Processed Data: " + value);
    
    flowFile = session.write(flowFile, new OutputStreamCallback() {

      @Override
      public void process(OutputStream out) throws IOException {
        out.write(value.getBytes(StandardCharsets.UTF_8));
      }
    });

    session.transfer(flowFile, REL_SUCCESS);
  }

}
