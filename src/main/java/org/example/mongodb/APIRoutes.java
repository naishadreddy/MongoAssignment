package org.example.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import spark.Request;
import spark.Response;
import com.google.gson.*;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Objects;
import java.util.stream.Collectors;

public class APIRoutes {
    
    private final MongoClient mongoClient ;
    private final MongoDatabase mongoDatabase ;
    
    APIRoutes(MongoClient mongoClient){
        this.mongoClient = mongoClient;
       this.mongoDatabase =  mongoClient.getDatabase("logistics");
    }

    JsonWriterSettings plainJSON = JsonWriterSettings.builder().outputMode(JsonMode.RELAXED)
            .binaryConverter((value, writer) -> writer.writeString(Base64.getEncoder().encodeToString(value.getData())))
            .dateTimeConverter((value, writer) -> {
                ZonedDateTime zonedDateTime = Instant.ofEpochMilli(value).atZone(ZoneOffset.UTC);
                writer.writeString(DateTimeFormatter.ISO_DATE_TIME.format(zonedDateTime));
            }).decimal128Converter((value, writer) -> writer.writeString(value.toString()))
            .objectIdConverter((value, writer) -> writer.writeString(value.toHexString()))
            .symbolConverter((value, writer) -> writer.writeString(value)).build();

    public Object getPlanes(Request req, Response res) {

        PlanesDAL planesDAL = new PlanesDAL(mongoClient);
        if(Objects.isNull(planesDAL.getAllPlanes())){
            res.status(404);
            return new Document("ok", false).append("error", "empty planes collection").toJson();
        }
        ArrayList<PlanesDAL> planes = planesDAL.getAllPlanes();
        ArrayList<Document> planesDoc = new ArrayList<Document>();
        for(PlanesDAL plane : planes){
            Document planeBSON = new Document("callsign",plane.getId())
                    .append("currentLocation",plane.getCurrentLocation())
                    .append("heading",plane.getHeading())
                    .append("landed",plane.getLanded())
                    .append("route",plane.getRoute());
            planesDoc.add(planeBSON);
        }
        return  new Gson().toJson(planesDoc);
    }

    public Object getPlaneById(Request req, Response res) {
        PlanesDAL planesDAL = new PlanesDAL(mongoClient);
        String planeId = req.splat()[0];
        PlanesDAL plane = planesDAL.getPlanesById(planeId);
        if(Objects.isNull(plane)){
            res.status(404);
            return new Document("ok", false).append("error", "no document found with given city name").toJson();
        }
        Document planeBSON  = new Document("callsign",plane.getId())
                .append("currentLocation",plane.getCurrentLocation())
                .append("heading",plane.getHeading())
                .append("landed",plane.getLanded())
                .append("route",plane.getRoute());

        return  new Gson().toJson(planeBSON);
    }

    public Object updatePlaneLocation(Request req, Response res) {
        PlanesDAL planesDAL = new PlanesDAL(mongoClient);
        String planeId = req.splat()[0];
        String location = req.splat()[1]                                                        ;
        String heading = req.splat()[2];
        Boolean isUpdated = planesDAL.updatePlaneLocation(planeId,location,heading);
        if(!isUpdated){
            res.status(404);
            String errorMsg = Objects.isNull(planesDAL.getLastError())? "invalid cargo id":planesDAL.getLastError();
            return new Document("ok", false).append("error" ,errorMsg).toJson();
        }

        res.status(200);
        return  "";
    }

    public Object updatePlaneLocationAndLanding(Request req, Response res) {
        PlanesDAL planesDAL = new PlanesDAL(mongoClient);
        String planeId = req.splat()[0];
        String location = req.splat()[1]                                                        ;
        String heading = req.splat()[2];
        String landed = req.splat()[3];
        Boolean isUpdated = planesDAL.updatePlaneLocationAndLanding(planeId,location,heading,landed);
        if(!isUpdated){
            res.status(404);
            String errorMsg = Objects.isNull(planesDAL.getLastError())? "invalid cargo id":planesDAL.getLastError();
            return new Document("ok", false).append("error" ,errorMsg).toJson();
        }

        res.status(200);
        return  "";
    }

    public Object addPlaneRoute(Request req, Response res, boolean b) {
            PlanesDAL planesDAL = new PlanesDAL(mongoClient);
            String planeId = req.splat()[0];
            String city = req.splat()[1];
            Boolean isUpdated = b ? planesDAL.replacePlaneRoute(planeId, city) : planesDAL.addPlaneRoute(planeId,city);
            if (!isUpdated) {
                res.status(404);
                String errorMsg = Objects.isNull(planesDAL.getLastError()) ? "invalid cargo id" : planesDAL.getLastError();
                return new Document("ok", false).append("error", errorMsg).toJson();
            }
            res.status(200);
            return "";
    }

    public Object removeFirstPlaneRoute(Request req, Response res) {
        PlanesDAL planesDAL = new PlanesDAL(mongoClient);
        String planeId = req.splat()[0];
        Boolean isUpdated = planesDAL.removeFirstPlaneRoute(planeId);
        if (!isUpdated) {
            res.status(404);
            String errorMsg = Objects.isNull(planesDAL.getLastError()) ? "invalid cargo id" : planesDAL.getLastError();
            return new Document("ok", false).append("error", errorMsg).toJson();
        }
        res.status(200);
        return "";
    }


    public Object getCities(Request req, Response res) {

        CitiesDAL citiesDAL = new CitiesDAL(mongoClient);
       if(Objects.isNull(citiesDAL.getCities())){
            res.status(404);
            return new Document("ok", false).append("error", "empty cities collection").toJson();
        }
        ArrayList<CitiesDAL> cities = citiesDAL.getCities();
       ArrayList<Document> cityDoc = new ArrayList<Document>();
       for(CitiesDAL city : cities){
           Document cityBSON = new Document("name",city.getId())
                                   .append("location",city.getPosition())
                                   .append("country",city.getCountry());
           cityDoc.add(cityBSON);
       }
        return  new Gson().toJson(cityDoc);
    }

    public Object getCityById(Request req, Response res) {


        System.out.print("inside getcitybyid methid ");
        CitiesDAL citiesDAL = new CitiesDAL(mongoClient);
        String city = req.splat()[0];

        if(Objects.isNull(citiesDAL.getCitiesById(city))){
            res.status(404);
            return new Document("ok", false).append("error", "no document found with given city name").toJson();
        }

        CitiesDAL cityBSON = citiesDAL.getCitiesById(city);
        return  new Gson().toJson(cityBSON);
    }

    public Object getNeighboringCitiesById(Request req, Response res) {

        System.out.print("inside negihbprinh methid ");
        CitiesDAL citiesDAL = new CitiesDAL(mongoClient);
        String city = req.splat()[0];
        Integer count = Integer.parseInt(req.splat()[1]);
        if(Objects.isNull(citiesDAL.getCities())){
            res.status(404);
            return new Document("ok", false).append("error", "empty cities collection").toJson();
        }



        ArrayList<CitiesDAL> cities = citiesDAL.getNeighboringCitiesById(city,count);;
        ArrayList<Document> cityDoc = new ArrayList<Document>();
        for(CitiesDAL c : cities){
            Document cityBSON = new Document("name",c.getId())
                    .append("location",c.getPosition())
                    .append("country",c.getCountry());
            cityDoc.add(cityBSON);
        }

        return new Gson().toJson(cityDoc);
    }

    public Object getCargoAtLocation(Request req, Response res) {
        CargoDAL cargoDAL = new CargoDAL(mongoClient);
        String location = req.splat()[0];

        ArrayList<Document> cargos = cargoDAL.getCargoByLocation(location);
        if(Objects.isNull(cargos)){
            res.status(404);
            return new Document("ok", false).append("error", cargoDAL.getLastError()).toJson();
        }
        return  new Gson().toJson(cargos);
    }

    public Object createCargo(Request req, Response res) {
        CargoDAL cargoDAL = new CargoDAL(mongoClient);
        String location = req.splat()[0];
        String destination = req.splat()[1];

       Document doc =  cargoDAL.createNewCargo(location,destination);
        if(Objects.isNull(doc)){
            res.status(404);
            return new Document("ok", false).append("error",cargoDAL.getLastError()).toJson();
        }

        return  doc.toJson();
    }

    public Object cargoDelivered(Request req, Response res) {
        CargoDAL cargoDAL = new CargoDAL(mongoClient);
        String cargoId = req.splat()[0];

        Boolean isUpdated = cargoDAL.cargoDelivered(cargoId);
        if (!isUpdated) {
            res.status(404);
            String errorMsg = Objects.isNull(cargoDAL.getLastError()) ? "invalid cargo id" : cargoDAL.getLastError();
            return new Document("ok", false).append("error", errorMsg).toJson();
        }
        res.status(200);
        return "";
    }

    public Object cargoAssignCourier(Request req, Response res) {
        CargoDAL cargoDAL = new CargoDAL(mongoClient);
        String cargoId = req.splat()[0];
        String planeId = req.splat()[1];

        Boolean isUpdated = cargoDAL.cargoAssignCourier(cargoId,planeId);
        if (!isUpdated) {
            res.status(404);
            String errorMsg = Objects.isNull(cargoDAL.getLastError()) ? "invalid cargo id" : cargoDAL.getLastError();
            return new Document("ok", false).append("error", errorMsg).toJson();
        }
        res.status(200);
        return "";
    }

    public Object cargoUnsetCourier(Request req, Response res) {
        CargoDAL cargoDAL = new CargoDAL(mongoClient);
        String cargoId = req.splat()[0];

        Boolean isUpdated = cargoDAL.cargoUnsetCourier(cargoId);
        if (!isUpdated) {
            res.status(404);
            String errorMsg = Objects.isNull(cargoDAL.getLastError()) ? "invalid cargo id" : cargoDAL.getLastError();
            return new Document("ok", false).append("error", errorMsg).toJson();
        }
        res.status(200);
        return "";
    }

    public Object cargoMove(Request req, Response res) {
        CargoDAL cargoDAL = new CargoDAL(mongoClient);
        String cargoId = req.splat()[0];

        Boolean isUpdated = cargoDAL.cargoUnsetCourier(cargoId);
        if (!isUpdated) {
            res.status(404);
            String errorMsg = Objects.isNull(cargoDAL.getLastError()) ? "invalid cargo id" : cargoDAL.getLastError();
            return new Document("ok", false).append("error", errorMsg).toJson();
        }
        res.status(200);
        return "";
    }
}
