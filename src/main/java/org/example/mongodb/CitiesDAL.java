package org.example.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.ReadConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class CitiesDAL {

    private final String ID ="_id";
    private final String COUNTRY ="country";
    private final String POSITION ="position";
    private final String INDEX_NAME ="position_2d";
    private final String DATABASE ="logistics";
    private final String COLLECTION ="cities";


    Logger logger;
    private String id;
    private String country;
    private ArrayList<Double> position;
    private String lastError;

    MongoClient mongoClient;
    MongoCollection<Document> cityCollection;

    public CitiesDAL(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        this.cityCollection = mongoClient.getDatabase(DATABASE).getCollection(COLLECTION).withReadConcern(ReadConcern.MAJORITY);
        this.logger =  LoggerFactory.getLogger(CitiesDAL.class);
    }

    public CitiesDAL(MongoClient mongoClient,String id, String country, ArrayList<Double> position) {
        this.mongoClient = mongoClient;
        this.cityCollection = mongoClient.getDatabase(DATABASE).getCollection(COLLECTION).withReadConcern(ReadConcern.MAJORITY);
        this.id = id;
        this.country = country;
        this.position = position;
    }

    public CitiesDAL(String id, String country, ArrayList<Double> position) {
        this.id = id;
        this.country = country;
        this.position = position;
    }

    public String getId() {
        return id;
    }

    public String getCountry() {
        return country;
    }

    public ArrayList<Double> getPosition() {
        return position;
    }

    public Boolean isCollectionEmpty(){
        if(Objects.isNull(cityCollection)) {
            this.lastError = "empty cities collection ";
            logger.error(this.lastError);
            return true;
        }
        return false;
    }

    List<Double> getCoordinatesFromCity(String city){
        if(isCollectionEmpty())
            return null;
        Document cityDoc =  cityCollection.find(Filters.eq(ID,city)).first();
        if(Objects.isNull(cityDoc) || Objects.isNull(cityDoc.get(POSITION)))
            return null;
        return (List<Double>)cityDoc.get(POSITION);
    }

    ArrayList<CitiesDAL> getCities() {
        if(isCollectionEmpty()){
            return null;
        }

        ArrayList<CitiesDAL> cities = new ArrayList<>();
        try(MongoCursor<Document> cursor = cityCollection.find().cursor()) {
            while (cursor.hasNext()){
                Document c = cursor.next();
                cities.add(new CitiesDAL(c.getString(ID),c.getString(COUNTRY), (ArrayList<Double>) c.get(POSITION)));
            }
        }
        return cities;
    }

    CitiesDAL getCitiesById(String cityName) {
        if(isCollectionEmpty()){
            return null;
        }

        Bson filter = Filters.eq(ID,cityName);
        Document cityDoc = cityCollection.find(filter).first();
        if(Objects.isNull(cityDoc))
            return null;
        return new CitiesDAL(cityDoc.getString(ID),cityDoc.getString(COUNTRY), (ArrayList<Double>) cityDoc.get(POSITION));
    }

    ArrayList<CitiesDAL> getNeighboringCitiesById(String cityName,int count) {

        if(isCollectionEmpty()){
            return null;
        }
        Bson filter = Filters.eq(ID,cityName);
        Document cityDoc = cityCollection.find(filter).first();
        if(Objects.isNull(cityDoc))
            return null;
        ArrayList<Double> positionArray = (ArrayList<Double>) cityDoc.get(POSITION);

        ArrayList<CitiesDAL> cities = new ArrayList<>();
        Document geoNearStage = new Document("$geoNear",new Document("near",new Document("type","coordinates")
                                                                                 .append("coordinates",positionArray))
                                                                                 .append("distanceField","distance")
                                                                                 .append("key",POSITION)
                                                                                 .append("spherical",false));
        Bson limitStage = Aggregates.limit(count);

        try(MongoCursor<Document> cursor = cityCollection.aggregate(Arrays.asList(
                geoNearStage,
                limitStage
        )).cursor()) {
            while (cursor.hasNext()){
                Document c = cursor.next();
                cities.add(new CitiesDAL(c.getString(ID),c.getString(COUNTRY), (ArrayList<Double>) c.get(POSITION)));
            }
        }
        return cities;
    }

}
