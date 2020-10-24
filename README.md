# NGSIToJson
This is a custom Apache Nifi (Fiware Draco) Processor, aims to convert incoming NGSIv2 HTTP notifications from Orion ContextBroker to Flat or Nested Json with or without requests metadata.  
This processor has provision to convert both type of `attrsFormat` payload mentioned in notification section of subscriptions created at Fiware Orion Context Broker.

## Getting Started

### Install
1. Install and run Apache Nifi (Fiware Draco), Follow [Draco Quick Start Guide](https://github.com/ging/fiware-draco/blob/master/docs/quick_start_guide.md).
2. Clone this repo 
	`git clone https://github.com/thebluemagician/NGSIToJson.git`
3. Change the directory, build the code base 
   `clean install`
4. Above step will create a nar file in `./nifi-ngsi2json-nar/target/`.
5. Move the generated nar to `lib` directory of draco.
6. Start the draco server `bin/nifi.sh start`

### Processor Configuration
`NGSIToJson` is configured through following parameters:

| Property Name             | Default Value | Allowable Values | Description                                                                                                    |
| ------------------------- | ------------- | ---------------- | ---------------------------------------------------------------------------------------------------------------|
| **Json Structure**        | Flat          |  Flat, Nested    | Specifies whether the output Json should be Flat or Nested                                                     |
| NGSI version              | v2            |                  | Only NGSIv2 is supported                                                                                       |
| **Include Metadata**      | False         |  True, False     | If true, the general metadata like- id, type, subscriptionId will be included in the FlowFile, Attributes metadata will be ignored|

### Usage
Changing the `Json Structure` and `Include Metadata` property will have impact on the contents of output flowfile. 
<br>
The notification generated at Orion Context Broker can be `normalized` or in `keyValues` format based on the subscriptions payload. The sample data can be found here:
- [normalized](doc/ngsiv2_normalized_data.txt)
- [keyValues](doc/ngsiv2_keyvalues_data.txt)

Properties Configuration scenarios are:
1. `Json Structure:Flat` and `Include Metadata:False`, Output flowFile will be in simple format:
```
{
    "timeStamp": "2020-06-04 19:00:00",
    "pm2p5": "201.10",
    "airQualityLevel": "SEVERE",
    "humidity": "61.45",
    "aqiPollutant": "PM2.5",
    "co": "0.72",
    "deviceId": "fixedEnv011-001"
}
```
2. `Json Structure:Flat` and `Include Metadata:True`, Output FlowFile will include metadata in base structure: 
```
{
    "timeStamp": "2020-06-04 19:00:00",
    "pm2p5": "201.10",
    "airQualityLevel": "SEVERE",
    "humidity": "61.45",
    "aqiPollutant": "PM2.5",
    "id": "fixedEnvAir",
    "co": "0.72",
    "type": "envAir",
    "subscriptionId": "5f9431ed5d8d27ba56582358",
    "deviceId": "fixedEnv011-001",
    "TimeInstant": "2020-10-24T15:03:10.734Z"
}
``` 
3. `Json Structure:Nested` and `Include Metadata:False`, Output FlowFile will have data with empty metadata objects:
```
{
    "metadata": {},
    "data": {
        "timeStamp": "2020-06-04 19:00:00",
        "pm2p5": "201.10",
        "airQualityLevel": "SEVERE",
        "humidity": "61.45",
        "aqiPollutant": "PM2.5",
        "co": "0.72",
        "deviceId": "fixedEnv011-001"
    }
}
```
4. `Json Structure:Nested` and `Include Metadata:True`, Output FlowFile will have both data and metadata populated objects:
```
{
    "metadata": {
        "id": "fixedEnvAir",
        "type": "envAir",
        "subscriptionId": "5f9431ed5d8d27ba56582358",
        "TimeInstant": "2020-10-24T15:06:27.305Z"
    },
    "data": {
        "timeStamp": "2020-06-04 19:00:00",
        "pm2p5": "201.10",
        "airQualityLevel": "SEVERE",
        "humidity": "61.45",
        "aqiPollutant": "PM2.5",
        "co": "0.72",
        "deviceId": "fixedEnv011-001"
    }
}
```
---
### Ref
1. [Apache Nifi Developer Guide](https://nifi.apache.org/developer-guide.html)
2. [NGSIv2 Walkthrough](https://fiware-orion.readthedocs.io/en/master/user/ngsiv2_implementation_notes/index.html)
