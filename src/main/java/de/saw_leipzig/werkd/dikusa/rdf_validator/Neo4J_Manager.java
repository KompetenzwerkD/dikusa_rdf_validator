package de.saw_leipzig.werkd.dikusa.rdf_validator;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import static org.neo4j.driver.Values.parameters;

public class Neo4J_Manager {
	
	public void initialize() {
				
	}
	
    

    public Neo4J_Manager( String uri, String user, String password )
    {
        driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );
    }
    
    private final Driver driver;
	
}