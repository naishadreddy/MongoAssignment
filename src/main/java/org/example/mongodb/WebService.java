package org.example.mongodb;

// import static spark.Spark.after;
// import static spark.Spark.put;
// import static spark.Spark.post;
// import static spark.Spark.get;
// import static spark.Spark.delete;
// import static spark.Spark.port;
import static spark.Spark.*;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.Charset;
import java.io.IOException;
import java.net.URISyntaxException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.util.logging.LogManager;

import com.mongodb.ConnectionString;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;


public class WebService {
	static final String version = "0.0.1";
	static Logger logger;
	private static String static_dir;

	public static int ordinalIndexOf(String str, String substr, int n) {
		int pos = -1;
		do {
			pos = str.indexOf(substr, pos + 1);
		} while (n-- > 0 && pos != -1);
		return pos;
	}

	public static void main(String[] args) {
		port(5000);
		static_dir = System.getProperty("user.dir");
		static_dir = static_dir.substring(0,ordinalIndexOf(static_dir,"/",2)) + "/static";
		externalStaticFileLocation(static_dir);
		LogManager.getLogManager().reset();
		
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();

		logger = LoggerFactory.getLogger(WebService.class);
		logger.info(version);

	    String URI="mongodb://localhost:27017";
        if(args.length > 0)
        {
                URI = args[0];

        }
        MongoClient mongoClient = createMongoClient();

		APIRoutes apiRoutes = new APIRoutes(mongoClient);
			// *** PLANES ***
				//Fetch planes
				// E.G. curl -X GET http://localhost:5000/planes
				get("/planes", apiRoutes::getPlanes);


				// Update location, heading, and landed for a plane
				// E.G. curl -X PUT http://localhost:5000/planes/CARGO10/location/2,3/240/London
				put("/planes/*/location/*/*/*", apiRoutes::updatePlaneLocationAndLanding);


				//Update location and heading for a plane
				// E.G. curl -X PUT http://localhost:5000/planes/CARGO10/location/2,3/240
				put("/planes/*/location/*/*", apiRoutes::updatePlaneLocation);



				//Fetch plane by ID
				// E.G. curl -X GET http://localhost:5000/planes/CARGO10
				get("/planes/*", apiRoutes::getPlaneById);





				//Replace a Plane's Route with a single city
				// E.G. curl -X PUT http://localhost:5000/planes/CARGO10/route/London
				put("/planes/*/route/*",(req,res) -> apiRoutes.addPlaneRoute(req,res,true));

				//Add a city to a Plane's Route
				// E.G. curl -X POST http://localhost:5000/planes/CARGO10/route/London
				post("/planes/*/route/*",(req,res) -> apiRoutes.addPlaneRoute(req,res,false));

				//Remove the first entry in the list of a Planes route
				// E.G. curl -X DELETE http://localhost:5000/planes/CARGO10/route/destination
				delete("/planes/*/route/destination", apiRoutes::removeFirstPlaneRoute);

			// ************

			
			// *** CITIES ***
				//Fetch ALL cities
				// E.G. curl -X GET http://localhost:5000/cities
				get("/cities", (req,res) -> apiRoutes.getCities(req,res));

			//Fetch n neighboring cities by ID
			// E.G. curl -X GET http://localhost:5000/cities/*/neighbors/*
			get("/cities/*/neighbors/*", apiRoutes::getNeighboringCitiesById);
			
				//Fetch City by ID
				// E.G. curl -X GET http://localhost:5000/cities/London
				get("/cities/*", apiRoutes::getCityById);


			// ************

			
			// *** CARGO ***
			// ************
				//Fetch Cargo by ID
				// E.G. curl -X GET http://localhost:5000/cargo/location/London
				get("/cargo/location/*", apiRoutes::getCargoAtLocation);


				// Create a new cargo at "location" which needs to get to "destination" - error if neither location nor destination exist as cities. Set status to "in progress" 
				// E.G. curl -X POST http://localhost:5000/cargo/London/to/Cairo
				post("/cargo/*/to/*", apiRoutes::createCargo);

				// Set status field to 'Delivered' - Increment some count of delivered items too.
				// E.G. curl -X PUT http://localhost:5000/cargo/5f45303156fd8ce208650caf/delivered
				put("/cargo/*/delivered", apiRoutes::cargoDelivered);

				// Mark that the next time the courier (plane) arrives at the location of this package it should be onloaded by setting the courier field - courier should be a plane.
				// E.G. curl -X PUT http://localhost:5000/cargo/5f45303156fd8ce208650caf/courier/CARGO10
				put("/cargo/*/courier/*", apiRoutes::cargoAssignCourier);

				// Unset the value of courier on a given piece of cargo
				// E.G. curl -X DELETE http://localhost:5000/cargo/5f4530d756fd8ce208650d83/courier
				delete("/cargo/*/courier", apiRoutes::cargoUnsetCourier);

				// Move a piece of cargo from one location to another (plane to city or vice-versa)
				// E.G. curl -X PUT http://localhost:5000/cargo/5f4530d756fd8ce208650d83/location/London
				put("/cargo/*/location/*", apiRoutes::cargoMove);

			after((req, res) -> {
				res.type("application/json");
			});
		

		return;
	}

	public static MongoClient createMongoClient() {

		MongoCredential credential = MongoCredential.createCredential("admin","admin","qwerty123".toCharArray());

		ConnectionString connectionString = new ConnectionString("mongodb+srv://admin:qwery123@cluster0.xvdsa.mongodb.net/admin?retryWrites=true&w=majority&readConcernLevel=majority");
		MongoClientSettings settings = MongoClientSettings.builder()
				.applyConnectionString(connectionString)
				.credential(credential)
				.build();

		MongoClient mongoClient = MongoClients.create(settings);
		return mongoClient;
	}

}
