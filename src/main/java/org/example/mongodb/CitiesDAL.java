package org.example.mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.ReadConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

public class CitiesDAL {

    private final String ID ="_id";
    private final String COUNTRY ="country";
    private final String POSITION ="position";
    private final String INDEX_NAME ="position_2d";
    private final String DATABASE ="logistics";
    private final String COLLECTION ="cities";



    private String id;
    private String country;
    private ArrayList<Double> position;

    MongoClient mongoClient;
    MongoCollection<Document> cityCollection;

    public CitiesDAL(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        this.cityCollection = mongoClient.getDatabase(DATABASE).getCollection(COLLECTION).withReadConcern(ReadConcern.MAJORITY);
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

     void create2dCoordinateIndex(){
        MongoCursor<Document> indexCursor =  cityCollection.listIndexes().cursor();
        while(indexCursor.hasNext()){
            if(indexCursor.next().getString("name").equalsIgnoreCase(INDEX_NAME));
              return;
        }
        cityCollection.createIndex(Indexes.geo2d(POSITION));
    }

    ArrayList<CitiesDAL> getCities() {
        if(Objects.isNull(cityCollection) || cityCollection.countDocuments()  == 0){
            return null;
        }

        ArrayList<CitiesDAL> cities = new ArrayList<CitiesDAL>();
        try(MongoCursor<Document> cursor = cityCollection.find().cursor()) {
            while (cursor.hasNext()){
                Document c = cursor.next();
                cities.add(new CitiesDAL(c.getString(ID),c.getString(COUNTRY), (ArrayList<Double>) c.get(POSITION)));
            }
        }
        return cities;
    }

    CitiesDAL getCitiesById(String cityName) {
        if(Objects.isNull(cityCollection) || cityCollection.countDocuments()  == 0){
            return null;
        }

        Bson filter = Filters.eq("_id",cityName);
        Document cityDoc = cityCollection.find(filter).first();
        if(Objects.isNull(cityDoc))
            return null;
        CitiesDAL city = new CitiesDAL(cityDoc.getString(ID),cityDoc.getString(COUNTRY), (ArrayList<Double>) cityDoc.get(POSITION));
        return city;
    }

    ArrayList<CitiesDAL> getNeighboringCitiesById(String cityName,int count) {

        if(Objects.isNull(cityCollection) || cityCollection.countDocuments()  == 0){
            return null;
        }

        Bson filter = Filters.eq(ID,cityName);
        Document cityDoc = cityCollection.find(filter).first();
        ArrayList<Double> postionArray = (ArrayList<Double>) cityDoc.get(POSITION);
        if(Objects.isNull(cityDoc))
            return null;

        ArrayList<CitiesDAL> cities = new ArrayList<CitiesDAL>();
        create2dCoordinateIndex();
        Document geoNearStage = new Document("$geoNear",new Document("near",new Document("type","coordinates").append("coordinates",postionArray))
                .append("key",POSITION).append("distanceField","'distance'").append("spherical",false));
        Bson limitStage = Aggregates.limit(count);
//        Bson projectStage = Aggregates.project( Projections.fields(
//                                                   Projections.excludeId(),
//                                                   Projections.computed("name","$_id"),
//                Projections.computed("location","$position"),
//                Projections.include("country")));

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
