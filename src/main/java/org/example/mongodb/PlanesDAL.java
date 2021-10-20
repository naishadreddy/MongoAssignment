package org.example.mongodb;

import com.mongodb.ReadConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.*;
import net.jodah.failsafe.Failsafe;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class PlanesDAL {

    private final String ID = "_id";
    private final String HEADING = "heading";
    private final String CURRENT_LOCATION = "currentLocation";
    private final String LANDED = "landed";
    private final String ROUTE = "route";
    private final String PREV_LANDED_CITY = "prevLanded";
    private final String LANDED_TIMESTAMP = "landedTimestamp";
    private final String PREV_LANDED_TIMESTAMP = "prevLandedTimestamp";
    private final String DATABASE = "logistics";
    private final String COLLECTION = "planes";
    private final String CITY_COLLECTION = "cities";
    private final String EMPTY_COLLECTION_MSG = "empty planes collection";


    private Logger logger;

    private String id;
    private ArrayList<String> route;
    private ArrayList<Double> currentLocation;
    private Integer heading;
    private String landed;
    private Date landedTimestamp;
    private String prevLanded;
    private Date prevLandedTimestamp;
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

    public PlanesDAL(String id, ArrayList<String> route, ArrayList<Double> currentLocation, Integer heading,String landed,Date landedTimestamp, MongoClient mongoClient) {
        this.id = id;
        this.route = route;
        this.currentLocation = currentLocation;
        this.heading = heading;
        this.mongoClient = mongoClient;
        this.landed =landed;
        this.landedTimestamp = landedTimestamp;
    }

    public PlanesDAL(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        this.planesCollection = mongoClient.getDatabase(DATABASE).getCollection(COLLECTION).withReadConcern(ReadConcern.MAJORITY);
        this.logger = LoggerFactory.getLogger(PlanesDAL.class);
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

    public Date getLandedTimestamp() {
        return landedTimestamp;
    }

    public MongoCollection getPlanesCollection() {
        return planesCollection;
    }

    public Boolean isCollectionEmpty(){
        if(Objects.isNull(planesCollection) ||  planesCollection.countDocuments() == 0) {
            this.lastError = "empty planes collection ";
            logger.error("empty planes collection ");
            return true;
        }
        return false;
    }

    public boolean isPlaneValid(String planeId) {
        if(Objects.isNull(planesCollection) ||  planesCollection.countDocuments() == 0)
            return false;

        Document cityDoc = (Document) planesCollection.find(Filters.eq(ID,planeId)).first();
        return !Objects.isNull(cityDoc);
    }


    public boolean isCityValid(String city) {
        CitiesDAL citiesDAL = new CitiesDAL(this.getMongoClient());
        return !Objects.isNull(citiesDAL.getCitiesById(city));
    }

    public List<Double> getCoordinatesFromCityName(String city) {
        CitiesDAL citiesDAL = new CitiesDAL(this.getMongoClient());
        return citiesDAL.getCoordinatesFromCity(city);
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

        Bson filter = Filters.and(Filters.eq(ID,planeId),Filters.eq(LANDED,location));
        Document checkDoc = (Document) planesCollection.find(filter).first();
        return !Objects.isNull(checkDoc);
    }

    public Boolean planeUpdateRequest(Bson filter, Bson updates){

        FindOneAndUpdateOptions findOneAndUpdateOptions = new FindOneAndUpdateOptions().upsert(false).returnDocument(ReturnDocument.AFTER);
        Document plane = (Document) planesCollection.findOneAndUpdate(filter, updates, findOneAndUpdateOptions);
        return !Objects.isNull(plane);
    }

    ArrayList<PlanesDAL> getAllPlanes() {
        if (isCollectionEmpty())
            return null;
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

    PlanesDAL getPlanesById(String planeId) {
        if (isCollectionEmpty())
            return null;
        Bson filter = Filters.eq(ID, planeId);
        Document plane = (Document) planesCollection.find(filter).first();
        if (Objects.isNull(plane))
            return null;
        PlanesDAL planeDoc = new
                PlanesDAL(plane.getString(ID), (ArrayList<String>) plane.get(ROUTE), (ArrayList<Double>) plane.get(CURRENT_LOCATION), plane.getInteger(HEADING),plane.getString(LANDED),plane.getDate(LANDED_TIMESTAMP), getMongoClient());
        return planeDoc;
    }

    Boolean updatePlaneLocation(String planeId,String location,String heading) {

        if (isCollectionEmpty())
            return false;
        try {
            List<String> locationArrayString =  Arrays.asList(location.split(","));
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
            System.out.println(" invalid double format from parameters " + e.getClass().getCanonicalName());
            return false;
        }
    }

    Boolean updatePlaneLocationAndLanding(String planeId,String location,String heading,String landedCity) {

       logger.info("updated received -->"+" plane "+planeId+" location updated " + landedCity+ " heading "+heading + " location "+location);

        if(isCollectionEmpty())
            return false;

        if(!isCityValid(landedCity)){
            this.lastError = "landed city is not valid "+ landedCity;
            logger.error("landed city is not valid "+ landedCity);
            return false;
        }
        try {
            List<String> locationArrayString =  Arrays.asList(location.split(","));
            List<Double> locationCasted = locationArrayString.stream().map(Double::parseDouble).collect(Collectors.toList());
            Integer headingCasted = Integer.parseInt(heading);
            String prevLanded = getPlanesById(planeId).getLanded();

            if(!landedCity.equalsIgnoreCase(prevLanded)) {
                Date prevLandedTimeStamp = getPlanesById(planeId).getLandedTimestamp();
                Bson filter = Filters.eq(ID, planeId.trim());
                Bson updates = new Document("$set", new Document(CURRENT_LOCATION, locationCasted)
                        .append(HEADING, headingCasted)
                        .append(LANDED, landedCity)
                        .append(PREV_LANDED_CITY, prevLanded)
                        .append(PREV_LANDED_TIMESTAMP, prevLandedTimeStamp)
                        .append(LANDED_TIMESTAMP, new Date()));

                // Retry mechanism to push updates
                Boolean isUpdated = Failsafe.with(CommonUtils.retryMethod()).get(() -> planeUpdateRequest(filter,updates));
                logger.error(" update status for updatePlaneLocationAndLanding "+isUpdated);
                return isUpdated;
            }
            else{
                this.lastError = "landedCity parameter given in API same as landedField in the document please check the update again ";
                return false;
            }
        }
        catch(Exception e){
            this.lastError = e.toString();
            logger.error(" error occurred while updating document " , e.toString(),e);
            return false;
        }
    }

    Boolean replacePlaneRoute(String planeId, String city) {

        if(isCollectionEmpty())
            return false;
        if(!isCityValid(city)){
            this.lastError = "destination city is not valid "+ city;
            logger.error("destination city is not valid "+ city);
            return false;
        }
        try {
            ArrayList<String> routeList = new ArrayList<String>();
            routeList.add(city);
            Bson filter = Filters.eq(ID, planeId.trim());
            Bson updates = new Document("$set", new Document(ROUTE, routeList));
            // retry updates
            Boolean isUpdated = Failsafe.with(CommonUtils.retryMethod()).get(() -> planeUpdateRequest(filter,updates));
            return isUpdated;
        }
        catch(Exception e){
            this.lastError = e.toString();
            logger.error(" invalid double format from parameters " + e.toString());
            return false;
        }
    }

    Boolean addPlaneRoute(String planeId, String city) {

        if(isCollectionEmpty())
            return false;
        if(!isCityValid(city)){
            this.lastError = "destination city is not valid "+ city;
            logger.error("destination city is not valid "+ city);
            return false;
        }
        try {
            Bson filter = Filters.eq(ID, planeId.trim());
            Bson updates = new Document("$addToSet", new Document(ROUTE, city.trim()));
            Boolean isUpdated = Failsafe.with(CommonUtils.retryMethod()).get(() -> planeUpdateRequest(filter,updates));
            return isUpdated;
        }
        catch(Exception e){
            this.lastError = e.toString();
            logger.error(" invalid double format from parameters " + e.toString());
            return false;
        }
    }

    Boolean removeFirstPlaneRoute(String planeId) {
        if(isCollectionEmpty())
            return false;
        if(!isPlaneLanded(planeId)){
            this.lastError = "plane not yet landed in the given destination please check again";
            logger.error("plane not yet landed in the given destination please check again");
            return false;
        }
        try {
            Bson filter = Filters.eq(ID, planeId.trim());
            Bson updates = new Document("$pop", new Document(ROUTE, -1));
            // retry array pop update
            Boolean isUpdated = Failsafe.with(CommonUtils.retryMethod()).get(() -> planeUpdateRequest(filter,updates));
            return isUpdated;
        }
        catch(Exception e){
            this.lastError = e.toString();
            return false;
        }
    }

    Boolean updateDistanceAndTime(String planeId) {
        if (Objects.isNull(planesCollection) || planesCollection.countDocuments() == 0){
            this.lastError = EMPTY_COLLECTION_MSG;
            return false;
        }
        try {
            Bson filter = Filters.eq(ID, planeId);
            Document plane = (Document) planesCollection.find(filter).first();
            if(Objects.isNull(plane))
                return false;
            String landedCity = plane.getString(LANDED);
            String prevLandedCity = plane.getString(PREV_LANDED_CITY);
            Date landedTimestamp = plane.getDate(LANDED_TIMESTAMP);
            Date prevLandedTimestamp = plane.getDate(PREV_LANDED_TIMESTAMP);

            List<Double> currentLandedCoordinates = getCoordinatesFromCityName(landedCity);
            List<Double> prevLandedCoordinates = getCoordinatesFromCityName(prevLandedCity);
            double distanceInKM =0.0 ;

            // first document previousLandedCoordintes check
            if(!Objects.isNull(prevLandedCoordinates))
                distanceInKM = distance(prevLandedCoordinates.get(0),currentLandedCoordinates.get(0),prevLandedCoordinates.get(1),currentLandedCoordinates.get(1));

            long durationInMs = 0;
            if(!Objects.isNull(prevLandedTimestamp))
                durationInMs = Math.abs(landedTimestamp.getTime() - prevLandedTimestamp.getTime());

            Bson updates = new Document("$inc", new Document("travelTimeMillis", durationInMs).append("distanceCoveredInMiles",distanceInKM));

            FindOneAndUpdateOptions findOneAndUpdateOptions = new FindOneAndUpdateOptions().upsert(false).returnDocument(ReturnDocument.AFTER);
            Document planeUpdated = (Document)  Failsafe.with(CommonUtils.retryMethod()).get(() -> planesCollection.findOneAndUpdate(filter, updates, findOneAndUpdateOptions));
            logger.info(planeUpdated.toJson());

            if(!Objects.isNull(planeUpdated) && planeUpdated.getDouble("distanceCoveredInMiles") > 50000.00){
               PlaneMaintenanceDAL planeMaintenanceDAL= new PlaneMaintenanceDAL(getMongoClient());
                planeMaintenanceDAL.addPlaneRecord(planeId.toString(),planeUpdated.getDouble("distanceCoveredInMiles"));
            }

            return !Objects.isNull(planeUpdated);
        }
        catch(Exception e){
            this.lastError = e.toString();
            logger.error(" error setting timeTaken and duration of plane  " + e.toString(),e);
            return false;
        }
    }

    public static double distance(double lat1, double lat2, double lon1, double lon2)
    {
        // The math module contains a function
        // named toRadians which converts from
        // degrees to radians.
        lon1 = Math.toRadians(lon1);
        lon2 = Math.toRadians(lon2);
        lat1 = Math.toRadians(lat1);
        lat2 = Math.toRadians(lat2);

        // Haversine formula
        double dlon = lon2 - lon1;
        double dlat = lat2 - lat1;
        double a = Math.pow(Math.sin(dlat / 2), 2)
                + Math.cos(lat1) * Math.cos(lat2)
                * Math.pow(Math.sin(dlon / 2),2);

        double c = 2 * Math.asin(Math.sqrt(a));

        // Radius of earth in kilometers. Use 3956
        // for miles
        double r = 3956;

        // calculate the result
        return(c * r);
    }
}
