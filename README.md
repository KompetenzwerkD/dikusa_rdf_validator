# dikusa_rdf_validator
Validator for RDF(star) data based on SHACL with import options into Dikusa central knowledge graph

## Build
* utilizes maven for building
* command: mvn clean package org.apache.maven.plugins:maven-dependency-plugin:copy-dependencies

## Run
* change into "target" directory
* command: java -jar RDF_Validator-1.0-SNAPSHOT.jar
* necessary parameters are described when executing the above command
* example:  java -jar RDF_Validator-1.0-SNAPSHOT.jar -s https://raw.githubusercontent.com/KompetenzwerkD/dikusa-core-ontology/main/core_ontology_schema.ttl -i https://raw.githubusercontent.com/KompetenzwerkD/dikusa-core-ontology/main/example_data/core_ontology_instance_data_IRI_with_error.ttl -v -e /home/user/data_export.ttls -r /home/user/shacl_report.ttls

