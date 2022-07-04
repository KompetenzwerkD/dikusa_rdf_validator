package de.saw_leipzig.werkd.dikusa.rdf_validator;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Transaction;

import static org.neo4j.driver.Values.parameters;

import java.util.List;

public class Neo4J_Manager {
	
	public void initialize() {
				
	}

    public Neo4J_Manager()
    {
        // you can provide Neo4J login data here
    	this( "uri", "user", "password" );
    }
	
    public Neo4J_Manager( String uri, String user, String password )
    {
        this.driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ) );
    }
    

    
    void printAll() {
    	Session session = this.driver.session();
    			// lists nodes from database foo
    			List list=session.run("MATCH (n) RETURN n").list();
    			for(int i=0;i<list.size();i++){
    			    System.out.println(list.get(i));
    			}
    			
    			
    }
    
    final Driver driver;
	
}