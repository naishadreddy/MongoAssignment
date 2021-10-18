package org.example.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Objects;

public class WorldCitiesDAL {

    private final String ID ="_id";
    private final String COUNTRY ="country";
    private final String CITY ="city";
    private final String CITY_ASCII ="city_ascii";
    private final String LAT ="lat";
    private final String LON ="lng";
    private final String DATABASE ="logistics";
    private final String COLLECTION ="worldcities";



    private ObjectId _id;
    private Long id;
    private String admin_name;
    private String capital;
    private String city;
    private String city_ascii;
    private String country;
    private String iso2;
    private String iso3;
    private Double lat;
    private Double lng;
    private BigInteger population;


    MongoClient mongoClient;
    MongoCollection<Document> worldCitiesCollection;


    public WorldCitiesDAL(MongoClient mongoClient){
        this.mongoClient = mongoClient;
        this.worldCitiesCollection = mongoClient.getDatabase(DATABASE).getCollection(COLLECTION);
    }

    public WorldCitiesDAL(ObjectId _id, Long id, String admin_name, String capital, String city, String city_ascii, String country, String iso2, String iso3, Double lat, Double lng, BigInteger population) {
        this._id = _id;
        this.id = id;
        this.admin_name = admin_name;
        this.capital = capital;
        this.city = city;
        this.city_ascii = city_ascii;
        this.country = country;
        this.iso2 = iso2;
        this.iso3 = iso3;
        this.lat = lat;
        this.lng = lng;
        this.population = population;
    }

    public ObjectId get_id() {
        return _id;
    }

    public Long getId() {
        return id;
    }

    public String getAdmin_name() {
        return admin_name;
    }

    public String getCapital() {
        return capital;
    }

    public String getCity() {
        return city;
    }

    public String getCity_ascii() {
        return city_ascii;
    }

    public String getCountry() {
        return country;
    }

    public String getIso2() {
        return iso2;
    }

    public String getIso3() {
        return iso3;
    }

    public Double getLat() {
        return lat;
    }

    public Double getLng() {
        return lng;
    }

    public BigInteger getPopulation() {
        return population;
    }

    String getCityFromCoordinates(Double lat,Double lng){
        if(Objects.isNull(worldCitiesCollection) ||  worldCitiesCollection.countDocuments() == 0 )
            return null;

        Document city =  worldCitiesCollection.find(Filters.and(Filters.eq(LAT,lat),Filters.eq(LON,lng))).first();
        if(Objects.isNull(city))
            return "";

        return city.getString(CITY);

    }

    String getCityFromCoordinates(ArrayList<Double> coordinates){
        if(Objects.isNull(worldCitiesCollection) ||  worldCitiesCollection.countDocuments() == 0
         || coordinates.size() != 2)
            return null;

        Document city =  worldCitiesCollection.find(Filters.and(Filters.eq(LAT,coordinates.get(0)),Filters.eq(LON,coordinates.get(1)))).first();
        if(Objects.isNull(city))
            return null;

        return city.getString(CITY);

    }

    Boolean isCityPresent(String city){
        if(Objects.isNull(worldCitiesCollection) ||  worldCitiesCollection.countDocuments() == 0)
            return false;

        Document cityDoc =  worldCitiesCollection.find(Filters.or(Filters.eq(CITY,city),Filters.eq(CITY_ASCII,city))).first();
        return !Objects.isNull(cityDoc);

    }


}
