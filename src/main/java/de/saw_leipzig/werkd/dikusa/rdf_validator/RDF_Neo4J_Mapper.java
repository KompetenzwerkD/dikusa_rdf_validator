package de.saw_leipzig.werkd.dikusa.rdf_validator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.rdf4j.common.transaction.IsolationLevels;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.vocabulary.RDF4J;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sail.memory.MemoryStore;
import org.eclipse.rdf4j.sail.shacl.ShaclSail;
import org.neo4j.driver.Session;

/*
 * Input: CSV RDF -> NeO4J
 * Input: Model
 * Output: Triple or Statements?
 */
public class RDF_Neo4J_Mapper {

	public RDF_Neo4J_Mapper(Model model, Neo4J_Manager manager, String mappingfilename) {

		this.model=model;
		
		ShaclSail shaclSail = new ShaclSail(new MemoryStore());
		
		this.sailRepository = new SailRepository(shaclSail);
		
		this.manager = manager;
		
		this.load_model_to_store();
		
		this.mappingfilename = mappingfilename;
				
	}
	
	private List<String[]> load_mappingfile() throws IOException { 
	    
		int count = 0;
	    
	    List<String[]> mappingfile = new ArrayList<>();
	    
		if ((this.mappingfilename.indexOf("http"))==0) {
			
			URL mappingURL = new URL(this.mappingfilename);
	        BufferedReader br = new BufferedReader(
	        new InputStreamReader(mappingURL.openStream()));
			String line = "";
	        while ((line = br.readLine()) != null) {
	            mappingfile.add(line.split(";"));
	        }
			
		}
		else {
			BufferedReader br = new BufferedReader(new FileReader(this.mappingfilename));
			String line = "";
	        while ((line = br.readLine()) != null) {
	            mappingfile.add(line.split(";"));
	        }
		}
		System.out.println("Mapping file successfully loaded from: "+this.mappingfilename+"\n");
	    return mappingfile;
	}

	private void load_model_to_store() {
		
		RepositoryConnection connection = sailRepository.getConnection();
		connection.begin();
		connection.add(this.model);
		connection.commit();
		connection.close();
	}
	
	/*
	 * according to mapping file data will be queried from in memory RDF-store and written to specified Neo4J-store 
	 */
	void map_data() {
		RepositoryConnection connection = sailRepository.getConnection();
		
		List<String[]> mappings = null;
		try {
			mappings = this.load_mappingfile();
			/*
			 * for (int i=0; i<mappings.size(); i++) {
			 * System.out.println("loaded from file "+mappings.get(i)[0]);
			 * System.out.println(mappings.get(i)[1]); }
			 */
		} catch (IOException e1) {
			System.out.println("Error: Mapping file could not be loaded!");
			e1.printStackTrace();
			System.exit(1);
		}
		

		
		/*String[][] mappings = {
				{"SELECT ?x WHERE { ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.werkd.saw-leipzig.de/person> }","Merge (a:Person {id: '?x'})"},
				{"SELECT ?x ?y ?z WHERE { ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.werkd.saw-leipzig.de/person> . ?x <http://www.werkd.saw-leipzig.de/has_preferred_name> ?y . ?y <http://www.werkd.saw-leipzig.de/has_name_value> ?z }","MERGE (a:Person {id: '?x'}) MERGE (b:Name_Event:Event {id: '?y'}) SET b.value = '?z' MERGE (a)-[:HAS_CLEAR_NAME]->(b)"},
				{"SELECT ?x ?y ?z WHERE { ?x <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.werkd.saw-leipzig.de/person> . ?x <http://www.werkd.saw-leipzig.de/has_birth> ?y . ?y <http://www.werkd.saw-leipzig.de/has_place> ?z }","MERGE (a:Person {id: '?x'}) MERGE (b:Birthplace_Event:Event {id: '?y'}) MERGE (c:Place {id: '?z'}) MERGE (a)-[:HAS_BIRTHPLACE]->(b) MERGE (b)-[:HAS_VALUE]->(c)"}};
		*/		
		
		// connect to rdf-store and create neo4j-session
		RepositoryConnection rdfconnection = sailRepository.getConnection();
		Session neosession = this.manager.driver.session();
		
		try {
	        	
	        	// for each mapping
	        	for (int j=0; j<mappings.size(); j++) {
	        	
	        		//query all instances of the current mapping from rdf-store
	        		
	        		//ArrayList<String> x = new ArrayList<String>();

	        		
	        		TupleQuery query = rdfconnection.prepareTupleQuery(mappings.get(j)[0]);
	        		TupleQueryResult qresult = query.evaluate();
	        		while (qresult.hasNext()) {
	        			BindingSet bindingSet = qresult.next();

	        		    String xx = new String();
	        			xx = bindingSet.getValue("x").stringValue();
	        			
	        		    String yy = new String();
	        		    String zz = new String();
	        		    
	        		    try {
	        		    	yy=bindingSet.getValue("y").stringValue();
	        		    }
	        		    catch (java.lang.NullPointerException e) {
	        		    	yy="";
	        		    }
	        		    try {
	        		    	zz=bindingSet.getValue("z").stringValue();
	        		    }
	        		    catch (java.lang.NullPointerException e) {
	        		    	zz="";
	        		    }
	        		    
	        		    //System.out.println(mappings.get(j)[1].replace("?x", xx).replace("?y", yy).replace("?z", zz));
	        		    
	        		    //insert statement
	        		    neosession.run(mappings.get(j)[1].replace("?x", xx).replace("?y", yy).replace("?z", zz));
	        		    	        		    
	        		}
	        		
	        	}
	        	
		}
		finally {
			neosession.close();
			connection.close();
			System.out.println("Data successfully exported to Neo4J\n");
		}
	}
	
	void query_repo(String queryString) {
		try (RepositoryConnection connection = this.sailRepository.getConnection()) {
			   queryString = "SELECT ?x ?y WHERE { ?x <http://www.werkd.saw-leipzig.de/has_preferred_name> ?z . ?z <http://www.werkd.saw-leipzig.de/has_name_value> ?y } ";
			   TupleQuery tupleQuery = connection.prepareTupleQuery(queryString);
			   try (TupleQueryResult result = tupleQuery.evaluate()) {
			      while (result.hasNext()) {
			         BindingSet bindingSet = result.next();
			         Value valueOfX = bindingSet.getValue("x");
			         Value valueOfY = bindingSet.getValue("y");
			         System.out.println(valueOfX+" "+valueOfY);
			      }
			   }
			   connection.close();
			}

	}

	private Model model;
	
	private String mappingfilename;
		
	private SailRepository sailRepository;
	
	private Neo4J_Manager manager;
}
