# Kafka Connect Transforms (Filter)

## Scope

This custom Kafka SMT (Single Message Transformation) is developed to filter or create tombstone Kafka records 
on a predefined filter. [JsonPath](https://github.com/json-path/JsonPath) expressions are used to refer to a JSON structure in each
record.

## Properties

| Name    | Description                                                                                                                                                                | Type   | Default value | Valid values                | Importance |
|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------|---------------|-----------------------------|------------|
| pattern | JsonPath expression (https://github.com/json-path/JsonPath) to match.                                                                                                       | String |               |                             | HIGH       |
| action  | Action to apply on filtered records. With the "nullify" option, the record value and schema is set to null, and can be used in conjunction to create tombstone records.    | String | include       | (nullify, include, exclude) | MEDIUM     |

## Examples

```properties
transforms=dropValue
transforms.dropValue.type=com.github.cnatsis.kafka.connect.transforms.Filter
transforms.dropValue.pattern=[?(@.my_field == "value")]
transforms.dropValue.action=nullify
```

## Packaging

This application is packaged in a .jar file (includes dependencies) using Maven.

```bash
mvn clean package
```

## Deployment

Deploy to Kafka Connect worker node and include .jar full path in the worker properties file (property `plugin.path`) 
