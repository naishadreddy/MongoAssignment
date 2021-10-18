package org.example.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.sql.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class PlanesDAL {

    private final String ID = "_id";
    private final String HEADING = "heading";
    private final String CURRENT_LOCATION = "currentLocation";
    private final String LANDED = "landed";
    private final String ROUTE = "route";
    private final String DATABASE = "logistics";
    private final String COLLECTION = "planes";
    private final String CITY_COLLECTION = "cities";
    private final String EMPTY_COLLECTION_MSG = "empty planes collection";


    private String id;
    private ArrayList<String> route;
    private ArrayList<Double> currentLocation;
    private Integer heading;
    private String landed;
    private String lastError;

    private MongoClient mongoClient;
    private MongoCollection planesCollection;

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public PlanesDAL(String id, ArrayList<String> route, ArrayList<Double> currentLocation, Integer heading,String landed, MongoClient mongoClient) {
        this.id = id;
        this.route = route;
        this.currentLocation = currentLocation;
        this.heading = heading;
        this.mongoClient = mongoClient;
        this.landed =landed;
    }

    public PlanesDAL(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        this.planesCollection = mongoClient.getDatabase(DATABASE).getCollection(COLLECTION);
    }

    public String getId() {
        return id;
    }

    public ArrayList<String> getRoute() {
        return route;
    }

    public ArrayList<Double> getCurrentLocation() {
        return currentLocation;
    }

    public Integer getHeading() {
        return heading;
    }

    public void setLanded(ArrayList<Double> currentLocation) {
        WorldCitiesDAL worldCitiesDAL = new WorldCitiesDAL(this.getMongoClient());
        String city = worldCitiesDAL.getCityFromCoordinates(currentLocation.get(0), currentLocation.get(1));
        this.landed = city;
    }

    public String getLanded() {
        return landed;
    }

    public String getLastError() {
        return lastError;
    }


    public boolean isPlaneValid(String planeId) {
        if(Objects.isNull(planesCollection) ||  planesCollection.countDocuments() == 0)
            return false;

        Document cityDoc = (Document) planesCollection.find(Filters.eq(ID,planeId)).first();
        return !Objects.isNull(cityDoc);
    }


    public boolean isCityValid(String city) {
        WorldCitiesDAL worldCitiesDAL = new WorldCitiesDAL(this.getMongoClient());
        return worldCitiesDAL.isCityPresent(city);
    }

    public boolean isPlaneLanded(String planeId) {

        Bson filterStage = Aggregates.match(Filters.eq(ID,planeId));
        Bson projectStage = Aggregates.project(Projections.fields(
                                                   Projections.excludeId(),
                                                   Projections.computed("firstCityLanded",new Document("$arrayElemAt",Arrays.asList("$route",0))),
                Projections.include(LANDED)));
        Bson finalProject = Aggregates.project(Projections.fields(Projections.computed("check",new Document("$eq",Arrays.asList("$firstCityLanded","$landed")))));
        Document checkDoc = (Document) planesCollection.aggregate(Arrays.asList(filterStage,projectStage,finalProject)).first();

        return !Objects.isNull(checkDoc) && checkDoc.getBoolean("check");
    }

    public boolean isPlaneLandedInLocation(String planeId,String location) {

        Bson filterStage = Filters.and(Filters.eq(ID,planeId));
        Bson projectStage = Aggregates.project(Projections.fields(
                Projections.excludeId(),
                Projections.computed("firstCityLanded",new Document("$arrayElemAt",Arrays.asList("$route",0))),
                Projections.include(LANDED)));
        Bson finalProject = Aggregates.project(Projections.fields(Projections.computed("check",new Document("$eq",Arrays.asList("$firstCityLanded","$landed")))));
        Document checkDoc = (Document) planesCollection.aggregate(Arrays.asList(filterStage,projectStage,finalProject)).first();

        return !Objects.isNull(checkDoc) && checkDoc.getBoolean("check");
    }

    ArrayList<PlanesDAL> getAllPlanes() {
        if (Objects.isNull(planesCollection) || planesCollection.countDocuments() == 0){
            this.lastError = EMPTY_COLLECTION_MSG;
            return null;
        }

        ArrayList<PlanesDAL> planes = new ArrayList<PlanesDAL>();

        try (MongoCursor<Document> cursor = planesCollection.find().cursor()) {
            while (cursor.hasNext()) {
                Document c = cursor.next();
                planes.add(new
                        PlanesDAL(c.getString(ID), (ArrayList<String>) c.get(ROUTE), (ArrayList<Double>) c.get(CURRENT_LOCATION), c.getInteger(HEADING),c.getString(LANDED),getMongoClient()));
            }
        }
        return planes;
    }

    PlanesDAL getPlanesById(String inputId) {
        if (Objects.isNull(planesCollection) || planesCollection.countDocuments() == 0){
            this.lastError = EMPTY_COLLECTION_MSG;
            return null;
        }

        Bson filter = Filters.eq(ID, inputId);
        Document plane = (Document) planesCollection.find(filter).first();
        if (Objects.isNull(plane))
            return null;
        PlanesDAL planeDoc = new
                PlanesDAL(plane.getString(ID), (ArrayList<String>) plane.get(ROUTE), (ArrayList<Double>) plane.get(CURRENT_LOCATION), plane.getInteger(HEADING),plane.getString(LANDED), getMongoClient());
        return planeDoc;
    }

    Boolean updatePlaneLocation(String planeId,String location,String heading) {

        if (Objects.isNull(planesCollection) || planesCollection.countDocuments() == 0){
            this.lastError = EMPTY_COLLECTION_MSG;
            return false;
        }
        List<String> locationArrayString =  Arrays.asList(location.split(","));
        try {
            List<Double> locationCasted = locationArrayString.stream().map(Double::parseDouble).collect(Collectors.toList());
            Integer headingCasted = Integer.parseInt(heading);
            Bson filter = Filters.eq(ID, planeId.trim());
            Bson updates = new Document("$set", new Document(CURRENT_LOCATION, locationCasted).append(HEADING, headingCasted));


            FindOneAndUpdateOptions findOneAndUpdateOptions = new FindOneAndUpdateOptions().upsert(false).returnDocument(ReturnDocument.AFTER);
            Document plane = (Document) planesCollection.findOneAndUpdate(filter, updates, findOneAndUpdateOptions);

            return !Objects.isNull(plane);
        }
        catch(Exception e){
            this.lastError = e.toString();
            System.out.println(" invalid double format from parameters " + e.toString());
            return false;
        }
    }

    Boolean updatePlaneLocationAndLanding(String planeId,String location,String heading,String landedCity) {

        if (Objects.isNull(planesCollection) || planesCollection.countDocuments() == 0){
            this.lastError = EMPTY_COLLECTION_MSG;
            return false;
        }
        List<String> locationArrayString =  Arrays.asList(location.split(","));
        if(!isCityValid(landedCity)){
            this.lastError = "landed city is not valid "+ landedCity;
            System.out.println("landed city is not valid "+ landedCity);
            return false;
        }


        try {
            List<Double> locationCasted = locationArrayString.stream().map(Double::parseDouble).collect(Collectors.toList());
            Integer headingCasted = Integer.parseInt(heading);
            Bson filter = Filters.eq(ID, planeId.trim());
            Bson updates = new Document("$set", new Document(CURRENT_LOCATION, locationCasted).append(HEADING, headingCasted).append(LANDED,landedCity));


            FindOneAndUpdateOptions findOneAndUpdateOptions = new FindOneAndUpdateOptions().upsert(false).returnDocument(ReturnDocument.AFTER);
            Document plane = (Document) planesCollection.findOneAndUpdate(filter, updates, findOneAndUpdateOptions);

            return !Objects.isNull(plane);
        }
        catch(Exception e){
            this.lastError = e.toString();
            System.out.println(" error occurred while updating document " + e.toString());
            return false;
        }
    }

    Boolean replacePlaneRoute(String planeId, String city) {

        if (Objects.isNull(planesCollection) || planesCollection.countDocuments() == 0){
            this.lastError = EMPTY_COLLECTION_MSG;
            return false;
        }
        if(!isCityValid(city)){
            this.lastError = "destination city is not valid "+ city;
            System.out.println("destination city is not valid "+ city);
            return false;
        }
        try {
            ArrayList<String> routeList = new ArrayList<String>();
            routeList.add(city);
            Bson filter = Filters.eq(ID, planeId.trim());
            Bson updates = new Document("$set", new Document(ROUTE, routeList));
            FindOneAndUpdateOptions findOneAndUpdateOptions = new FindOneAndUpdateOptions().upsert(false).returnDocument(ReturnDocument.AFTER);
            Document plane = (Document) planesCollection.findOneAndUpdate(filter, updates, findOneAndUpdateOptions);
            return !Objects.isNull(plane);
        }
        catch(Exception e){
            this.lastError = e.toString();
            System.out.println(" invalid double format from parameters " + e.toString());
            return false;
        }
    }

    Boolean addPlaneRoute(String planeId, String city) {

        if (Objects.isNull(planesCollection) || planesCollection.countDocuments() == 0){
            this.lastError = EMPTY_COLLECTION_MSG;
            return false;
        }
        if(!isCityValid(city)){
            this.lastError = "destination city is not valid "+ city;
            System.out.println("destination city is not valid "+ city);
            return false;
        }
        try {

            Bson filter = Filters.eq(ID, planeId.trim());
            Bson updates = new Document("$addToSet", new Document(ROUTE, city));
            FindOneAndUpdateOptions findOneAndUpdateOptions = new FindOneAndUpdateOptions().upsert(false).returnDocument(ReturnDocument.AFTER);
            Document plane = (Document) planesCollection.findOneAndUpdate(filter, updates, findOneAndUpdateOptions);
            return !Objects.isNull(plane);
        }
        catch(Exception e){
            this.lastError = e.toString();
            System.out.println(" invalid double format from parameters " + e.toString());
            return false;
        }
    }

    Boolean removeFirstPlaneRoute(String planeId) {
        if (Objects.isNull(planesCollection) || planesCollection.countDocuments() == 0){
            this.lastError = EMPTY_COLLECTION_MSG;
            return false;
        }

        if(!isPlaneLanded(planeId)){
            this.lastError = "plane not yet landed in the given destination please check again";
            return false;
        }

        try {


            Bson filter = Filters.eq(ID, planeId.trim());
            Bson updates = new Document("$pop", new Document(ROUTE, -1));
            FindOneAndUpdateOptions findOneAndUpdateOptions = new FindOneAndUpdateOptions().upsert(false).returnDocument(ReturnDocument.AFTER);
            Document plane = (Document) planesCollection.findOneAndUpdate(filter, updates, findOneAndUpdateOptions);
            return !Objects.isNull(plane);
        }
        catch(Exception e){
            this.lastError = e.toString();
            System.out.println(" error deleting the route from  document " + e.toString());
            return false;
        }
    }

}
