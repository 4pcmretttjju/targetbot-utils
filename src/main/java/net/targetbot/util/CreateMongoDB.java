package net.targetbot.util;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.nio.charset.StandardCharsets; 
import java.nio.file.*; 
import java.io.*; 

public class CreateMongoDB {

    private static String[][] suburbs = new String[10][10];
    private static String[][] places = new String[100][100];
    private static String[][] wikinames = new String[100][100];
    private static String[][] buildingtypes = new String[100][100];

    public static void main(String[] args) {
    	
    	//	Read map data from file into static arrays
    	// String mapFilename = System.getProperty("mapdata.filename");
    	// assert(mapFilename != null && mapFilename.length() > 0);
    	readMap("src/main/resources/mapdata.txt");
    	
    	//	Database details are provided by VM runtime variables
    	String connectionString = System.getProperty("mongodb.uri");
    	String databaseString = System.getProperty("mongodb.database");
    	
    	assert(connectionString != null);
    	assert(databaseString  != null);
    	
    	//	Write suburb and block data into two new collections in a new database
    	createMongoDB(connectionString, databaseString);
    	
        return;        
    }
    
    //	Read UD suburb names, block / wiki names and building types from file
    //	Store in static arrays
    private static void readMap(String fileName) {
    	
    	List<String> lines = Collections.emptyList();
    	try {lines = Files.readAllLines(Paths.get(fileName), StandardCharsets.UTF_8);}
    	catch (IOException e) {e.printStackTrace();} 
    	Iterator<String> itr = lines.iterator(); 
        
    	//	1) Read 100 suburb names
    	for (int y = 0; y < 10; y++) {
    		for (int x = 0; x < 10; x++) {
    			suburbs[x][y] = itr.next();
    		}
    	}
    	
    	//	2) Read 10,000 block names
		for (int x = 0; x < 100; x++) {
	    	for (int y = 0; y < 100; y++) {
    			places[x][y] = itr.next();
    		}
    	}
    	
    	//	3) Read 10,000 wiki names
		for (int x = 0; x < 100; x++) {
	    	for (int y = 0; y < 100; y++) {
    			wikinames[x][y] = itr.next();
    		}
    	}
    	
    	//	4) Read 10,000 building types
		for (int x = 0; x < 100; x++) {
	    	for (int y = 0; y < 100; y++) {
    			buildingtypes[x][y] = itr.next();
    		}
    	}
		
    	return;
    }
    
    //	Create two new collections in the specified database, and populate with map data
    private static void createMongoDB(String connection, String database) {

        Logger.getLogger("org.mongodb.driver").setLevel(Level.WARNING);
        try (MongoClient mongoClient = MongoClients.create(connection)) {

            MongoDatabase mongoDB = mongoClient.getDatabase(database);
            assert(mongoDB != null);
            
            //	Create "Suburbs" database collection
            mongoDB.getCollection("Suburbs").insertMany(suburbList());           
            
            //	Create "Blocks" database collection
            mongoDB.getCollection("Blocks").insertMany(blockList());            
        }    	
        return;
    }
    
    //	Create list of BSON documents for bulk insertion into a MongoDB collection
    private static List<Document> suburbList() {
    	
    	List<Document> docList = new ArrayList<>();
    	
    	for (int x = 0; x < suburbs.length; x++) {
        	for (int y = 0; y < suburbs[x].length; y++) {
        		Document doc = suburbDocument(x, y);
        		docList.add(doc);
        	}    		
    	}
    	return docList;
    }
    
    //	Create list of BSON documents for bulk insertion into a MongoDB collection
    private static List<Document> blockList() {
    	
    	List<Document> docList = new ArrayList<>();
    	
    	for (int x = 0; x < places.length; x++) {
        	for (int y = 0; y < places[x].length; y++) {
        		Document blockDoc = blockDocument(x, y);
        		docList.add(blockDoc);
        	}    		
    	}
    	return docList;
    }
    
    //	Create BSON document for insertion into a MongoDB collection
    private static Document suburbDocument(int x, int y) {
    	
    	assert(x >= 0 && x < 10);
    	assert(y >= 0 && y < 10);

    	Document suburbDoc = new Document("_id", new ObjectId());
    	suburbDoc.append("x", x);
    	suburbDoc.append("y", y);
    	suburbDoc.append("name", suburbs[x][y]);
              
        return suburbDoc;
    }
    
    //	Create BSON document for insertion into a MongoDB collection
    private static Document blockDocument(int x, int y) {
    	
    	assert(x >= 0 && x < 100);
    	assert(y >= 0 && y < 100);

    	Document block = new Document("_id", new ObjectId());
        block.append("block_id", x + y*100);
        block.append("x", x);
        block.append("y", y);
        block.append("name", places[x][y]);
        block.append("wikipage", wikinames[x][y]);
        block.append("type", buildingtypes[x][y]);
        return block;
    }
}
