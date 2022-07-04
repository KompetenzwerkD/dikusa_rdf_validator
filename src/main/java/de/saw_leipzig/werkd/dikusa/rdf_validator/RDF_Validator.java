package de.saw_leipzig.werkd.dikusa.rdf_validator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
//import java.util.Arrays;
//import java.util.List;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
import org.eclipse.rdf4j.common.exception.ValidationException;
import org.eclipse.rdf4j.common.transaction.IsolationLevels;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.TreeModel;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.model.vocabulary.RDF4J;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.UnsupportedRDFormatException;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.shacl.ShaclSail;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.MissingOptionException;


public class RDF_Validator {

    public static void main(String[] args) {
   
        RDF_Validator validator = new RDF_Validator();
        
        validator.start(args);

    }
    
    /*
     * starts the validation process
     */
    private void start(String[] args) {

        this.initialize();
        try {
			this.load_parameters(args);
		} catch (MissingOptionException e3) {
			System.out.println("Error: missing command-line argument(s)");
            System.out.println("Please, follow these instructions:");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "RDF Validator", this.options );
			System.exit(1);
			
		} catch (ParseException e2) {
			System.out.println("Error parsing command-line argument(s)");
            System.out.println("Please, follow these instructions:");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "RDF Validator", this.options );
			System.exit(1);
		} 
        
        try {
			this.load_models();
		} catch (IOException e1) {
			System.out.println("Error while accessing file input, see following error message:\n");
			e1.printStackTrace();
			System.exit(1);
		}
        this.enrich();
        if (validate_bool==true) {
        	this.validate();
        }
        
        try {
			this.write_output();
		} catch (RDFHandlerException | UnsupportedRDFormatException | FileNotFoundException | URISyntaxException e) {
			System.out.println("Error writing results to file, see following error message\n");
			e.printStackTrace();
			System.exit(1);
		}
        
        if (validation_failed==true) {
        	System.exit(1);
        }
        
        if (this.mapping_bool==true) {
        	this.map_and_export();
        }
                
    }
    
    /*
     * maps RDF-data in DIKUSA common data model to Neo4J and exports the results
     */
    private void map_and_export() {
    	
    	RDF_Neo4J_Mapper mapper = new RDF_Neo4J_Mapper(this.instance_model, this.neo,  this.mapping_filename);
    	
    	mapper.map_data();
    	
    	//this.neo.printAll();
	}
    
    /*
     * writes (enriched) RDF data and validation report data to file
     */
	private void write_output() throws RDFHandlerException, UnsupportedRDFormatException, FileNotFoundException, URISyntaxException {
		
		
		if (export_bool==true) {
			Rio.write(this.instance_model, new FileOutputStream(export_file), "", RDFFormat.TURTLESTAR);
			System.out.println("RDF data written to: "+export_file+"\n");
		}
		
		if ((report_bool==true) && (this.validation_failed==true)) {
	    	Rio.write(this.validationReportModel, new FileOutputStream(report_file), RDFFormat.TURTLESTAR);
			System.out.println("Validation report written to: "+report_file+"\n");
		}
		
	}

	/*
	 * Validate RDF data against SHACL constraints in RDF schema
	 */
	private void validate() {

		ShaclSail shaclSail = new ShaclSail(new MemoryStore());
		SailRepository sailRepository = new SailRepository(shaclSail);	
		RepositoryConnection connection = sailRepository.getConnection();
		connection.begin(IsolationLevels.NONE, ShaclSail.TransactionSettings.ValidationApproach.Auto);

		connection.add(schema_model, RDF4J.SHACL_SHAPE_GRAPH);
		connection.add(instance_model);

		// commit transaction and catch any exception
		try {
			connection.commit();
			//sailRepository.shutDown();
			System.out.println("Validation great success!!!\n");

			
			
		} catch (RepositoryException e){
			if(e.getCause() instanceof ValidationException){
				
				this.validation_failed = true;
				
				System.out.println("Oh no, validation failed!!! Please examine the following report:\n");
				
				this.validationReportModel = ((ValidationException) e.getCause()).validationReportAsModel();
				
				Rio.write(this.validationReportModel, System.out, RDFFormat.TURTLESTAR);
				
				System.out.println("\n");

				/*
				 * validationReportModel .filter(null, SHACL.SOURCE_SHAPE, null) .forEach(s -> {
				 * Value object = s.getObject();
				 * 
				 * 
				 * try (Stream<Statement> stream = connection.getStatements((Resource) object,
				 * null, null, RDF4J.SHACL_SHAPE_GRAPH).stream()) { List<Statement> collect =
				 * stream.collect(Collectors.toList());
				 * 
				 * // collect contains the shape!
				 * System.out.println(Arrays.toString(collect.toArray())); }
				 * 
				 * 
				 * 
				 * });
				 */
				
			}

		}


	}

	/*
	 * Enriches the RDF data with all possible inverse statements and symmetric statements; also copies subClassOf-statements from schema
	 */
	private void enrich() {
		
		// create temporary model for new statements
    	Model instance2_model = new TreeModel();
    	
    	// insert inverse relations and symmetric relations into temporary model
		for (Statement statement: this.instance_model) {
    	    Resource subject = statement.getSubject();
    	    IRI property = statement.getPredicate();
    	    Value object = statement.getObject();
    	    // inverse relations
    	    if (object.isIRI() && !property.toString().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") && !property.toString().equals("http://www.w3.org/2000/01/rdf-schema#subClassOf") && !(property.toString().contains("is_same")&&property.toString().endsWith("as"))) {
    	    	//System.out.println(statement);
    	    	if (property.toString().endsWith("_i")) {
    	    		instance2_model.add(Values.iri(object.stringValue()),Values.iri(property.stringValue().substring(0, (property.stringValue().length()-2))), subject);
    	    	}
    	    	else {
    	    		instance2_model.add(Values.iri(object.stringValue()),Values.iri(property.stringValue().concat("_i")), subject);
    	    	}
    	    }
    	    // symmetric relations
    	    else if (property.toString().contains("is_same")&&property.toString().endsWith("as")) {
    	    	instance2_model.add(Values.iri(object.stringValue()),property, subject);
    	    }
    	}

    	//insert all subclass relations of the schema model into the instance model (for SHACL)
    	for (Statement statement: this.schema_model) {
    	    
    	    IRI property = statement.getPredicate();
    	    if (property.toString().equals("http://www.w3.org/2000/01/rdf-schema#subClassOf")) {
        	    this.instance_model.add(statement);
    	    }
    	    
    	}

    	//append statements in temporary model to instance model
    	for (Statement statement: instance2_model) {
    		this.instance_model.add(statement);
    	}
		
	}

	/*
	 * loads RDF schema and RDF data file from local file system or URL
	 */
	private void load_models() throws IOException {

		if ((this.instance_file.indexOf("http"))==0) {
			URL instance_documentUrl = new URL(instance_file);
			//https://raw.githubusercontent.com/dgoldhahn/dikusa_core_visualization_test/main/core_test_instance.ttl
	    	InputStream instance_inputStream = instance_documentUrl.openStream();
	    	this.instance_model = Rio.parse(instance_inputStream, "", RDFFormat.TURTLESTAR);
	    	instance_inputStream.close();
		}
		else {
	        InputStream instance_inputStream = new FileInputStream(new File(instance_file));
	        this.instance_model = Rio.parse(instance_inputStream, "", RDFFormat.TURTLESTAR);
	    	instance_inputStream.close();
			
		}

		if ((schema_file.indexOf("http"))==0) {
	    	URL schema_documentUrl = new URL(schema_file);
	    	//https://raw.githubusercontent.com/KompetenzwerkD/dikusa-core-ontology/main/core_ontology_schema.ttl
	    	InputStream schema_inputStream = schema_documentUrl.openStream();
	    	this.schema_model = Rio.parse(schema_inputStream, "", RDFFormat.TURTLESTAR);
	    	schema_inputStream.close();
		}
		else {
	        InputStream schema_inputStream = new FileInputStream(new File(schema_file));
	        this.schema_model = Rio.parse(schema_inputStream, "", RDFFormat.TURTLESTAR);
	    	schema_inputStream.close();
		}
		
		System.out.println("All RDF files successfully loaded\n");
    	

	}

	/*
	 * loads all command line parameters
	 */
	private void load_parameters(String[] arguments) throws ParseException, MissingOptionException {
		//Create a parser
		CommandLineParser parser = new DefaultParser();
		
		//parse the options passed as command line arguments
		cmd = parser.parse( options, arguments);
		
        if (cmd.hasOption("i")) {
            this.instance_file = cmd.getOptionValue("i");
            System.out.println("\nRDF data file to be loaded: " + instance_file + "\n");
        }
        
        if (cmd.hasOption("s")) {
            this.schema_file = cmd.getOptionValue("s");
            System.out.println("RDF schema file to be loaded: " + schema_file + "\n");
        }
        
        if (cmd.hasOption("e")) {
            this.export_bool = true;
            this.export_file = cmd.getOptionValue("e");
            System.out.println("RDF data to be exported to local file: " + this.export_file + "\n");
        }
		
        if (cmd.hasOption("v")) {
            this.validate_bool = true;
            System.out.println("RDF data will be validated\n");
        }
        
        if (cmd.hasOption("r")) {
            this.report_bool = true;
            this.report_file = cmd.getOptionValue("r");
            System.out.println("Shacl validation report will be exported to local file: " + this.report_file + "\n");
        }

        if (cmd.hasOption("m")) {
            this.mapping_bool = true;
            this.mapping_filename = cmd.getOptionValue("m");
            System.out.println("Mapping file will be loaded from: " + this.mapping_filename + "\n");
        }
	}
	
	/*
	 * initialize variables; initialize Options object for managing command line parameters
	 */
	private void initialize() {
		this.instance_model = new TreeModel();
		this.schema_model = new TreeModel();
		
		this.instance_file = new String();
		this.schema_file = new String();
		
		this.export_bool = false;
		this.export_file = new String();
		
		this.validate_bool = false;
		
		this.report_bool = false;
		this.report_file = new String();
		
		this.validationReportModel = new TreeModel();
		
		this.validation_failed = false;
		
		this.mapping_filename = new String();
		this.mapping_bool = false;
		
		this.neo = new Neo4J_Manager();
		
		//you can comment out the line above and provide Neo4J login data here
		//this.neo = new Neo4J_Manager("neo4j://server:7687","user","pass");
		
		// create Options object
		this.options = new Options();

        this.options.addOption(Option.builder("i")
                .longOpt("instance")
                .hasArg(true)
                .desc("location (URL or local) of RDF data file in TURTLE-star format for import [required]")
                .required(true)
                .build());
        this.options.addOption(Option.builder("s")
                .longOpt("schema")
                .hasArg(true)
                .desc("location (URL or local) of RDF schema file in TURTLE-star syntax for import [required]")
                .required(true)
                .build());
        this.options.addOption(Option.builder("e")
                .longOpt("export")
                .hasArg(true)
                .desc("specify local export location for enriched RDF file (in TURTLE-star syntax)")
                .required(false)
                .build());
        this.options.addOption(Option.builder("v")
                .longOpt("validate")
                .hasArg(false)
                .desc("validate RDF file")
                .required(false)
                .build());
        this.options.addOption(Option.builder("r")
                .longOpt("report")
                .hasArg(true)
                .desc("specify local export location for shacl validation report (if validation fails)")
                .required(false)
                .build());
        this.options.addOption(Option.builder("m")
        		.longOpt("mapping")
                .hasArg(true)
                .desc("location (URL or local) of mapping file in csv format for knowledge base import")
                .required(false)
                .build());
		
	}
	
	private Model instance_model;
	
	private Model schema_model;
	
	private Options options;
	
	private CommandLine cmd;
	
	private String instance_file;
	
	private String schema_file;
	
	private Boolean export_bool;
	
	private String export_file;
	
	private Boolean validate_bool;

	private Boolean report_bool;
	
	private String report_file;
	
	private Model validationReportModel;
	
	private Boolean validation_failed;
	
	private String mapping_filename;
	
	private Neo4J_Manager neo;
	
	private Boolean mapping_bool;
}
