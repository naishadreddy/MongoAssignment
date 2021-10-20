package org.example.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import com.google.gson.*;


import java.util.ArrayList;
import java.util.Objects;

public class APIRoutes {
    
    private final MongoClient mongoClient ;
    private final MongoDatabase mongoDatabase ;
    private final static Logger logger = LoggerFactory.getLogger(APIRoutes.class);
    
    APIRoutes(MongoClient mongoClient){
        this.mongoClient = mongoClient;
       this.mongoDatabase =  mongoClient.getDatabase("logistics");
    }

    public Object getPlanes(Request req, Response res) {

        PlanesDAL planesDAL = new PlanesDAL(mongoClient);
        if(Objects.isNull(planesDAL.getAllPlanes())){
            res.status(404);
            logger.error(planesDAL.getLastError());
            return new Document("ok", false).append("error", planesDAL.getLastError()).toJson();
        }
        ArrayList<PlanesDAL> planes = planesDAL.getAllPlanes();
        ArrayList<Document> planesDoc = new ArrayList<>();
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
            logger.error(planesDAL.getLastError());
            return new Document("ok", false).append("error", planesDAL.getLastError()).toJson();
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
            logger.error(" error updating plane location "+planesDAL.getLastError());
            return new Document("ok", false).append("error", planesDAL.getLastError()).toJson();
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
            String errorMsg = Objects.isNull(planesDAL.getLastError())? "invalid plane ID":planesDAL.getLastError();
            logger.error(" error updating plane location and landing" + planesDAL.getLastError());
            return new Document("ok", false).append("error", planesDAL.getLastError()).toJson();
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
                logger.error(" error adding plane route " + planesDAL.getLastError());
                return new Document("ok", false).append("error", planesDAL.getLastError()).toJson();
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
            logger.error(" error removing plane route " + planesDAL.getLastError());
            return new Document("ok", false).append("error", planesDAL.getLastError()).toJson();
        }
        res.status(200);
        return "";
    }


    public Object getCities(Request req, Response res) {

        CitiesDAL citiesDAL = new CitiesDAL(mongoClient);
       if(Objects.isNull(citiesDAL.getCities())){
            res.status(404);
            logger.error("empty cities collection");
            return new Document("ok", false).append("error", "empty cities collection").toJson();
        }
        ArrayList<CitiesDAL> cities = citiesDAL.getCities();
       ArrayList<Document> cityDoc = new ArrayList<>();
       for(CitiesDAL city : cities){
           Document cityBSON = new Document("name",city.getId())
                                   .append("location",city.getPosition())
                                   .append("country",city.getCountry());
           cityDoc.add(cityBSON);
       }
        return  new Gson().toJson(cityDoc);
    }

    public Object getCityById(Request req, Response res) {

        CitiesDAL citiesDAL = new CitiesDAL(mongoClient);
        String city = req.splat()[0];
        CitiesDAL cityDAL = citiesDAL.getCitiesById(city);
        if(Objects.isNull(cityDAL)){
            res.status(404);
            logger.error("city not found");
            return new Document("ok", false).append("error", "no document found with given city name").toJson();
        }

        Document cityBSON = new Document("name",cityDAL.getId())
                .append("location",cityDAL.getPosition())
                .append("country",cityDAL.getCountry());

        return  new Gson().toJson(cityBSON);
    }

    public Object getNeighboringCitiesById(Request req, Response res) {
        CitiesDAL citiesDAL = new CitiesDAL(mongoClient);
        String city = req.splat()[0];
        Integer count = Integer.parseInt(req.splat()[1]);
        if(Objects.isNull(citiesDAL.getCities())){
            res.status(404);
            logger.error("empty cities collection");
            return new Document("ok", false).append("error", "empty cities collection").toJson();
        }
        ArrayList<CitiesDAL> cities = citiesDAL.getNeighboringCitiesById(city,count);
        ArrayList<Document> cityDoc = new ArrayList<>();
        for(CitiesDAL c : cities){
            Document cityBSON = new Document("name",c.getId())
                    .append("location",c.getPosition())
                    .append("country",c.getCountry());
            cityDoc.add(cityBSON);
        }

        return  new Document("neighbors", cityDoc).toJson();
    }

    public Object getCargoAtLocation(Request req, Response res) {
        CargoDAL cargoDAL = new CargoDAL(mongoClient);
        String location = req.splat()[0];
        ArrayList<Document> cargos = cargoDAL.getCargoByLocation(location);
        if(Objects.isNull(cargos)){
            res.status(404);
            logger.error(cargoDAL.getLastError());
            return  new Gson().toJson(new ArrayList<>());
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
            logger.error("creating cargo failed "+cargoDAL.getLastError());
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
            String errorMsg = cargoDAL.getLastError();
            logger.error("error updating cargo destination " +errorMsg);
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
            String errorMsg = cargoDAL.getLastError();
            logger.error("error assigning courier to cargo " +errorMsg);
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
            String errorMsg =  cargoDAL.getLastError();
            logger.error("error setting courier to cargo " +errorMsg);
            return new Document("ok", false).append("error", errorMsg).toJson();
        }
        res.status(200);
        return "";
    }

    public Object cargoMove(Request req, Response res) {
        CargoDAL cargoDAL = new CargoDAL(mongoClient);
        String cargoId = req.splat()[0];
        String location = req.splat()[1];

        Boolean isUpdated = cargoDAL.cargoMove(cargoId,location);
        if (!isUpdated) {
            res.status(404);
            String errorMsg = cargoDAL.getLastError();
            logger.error("error moving cargo " +errorMsg);
            return new Document("ok", false).append("error", errorMsg).toJson();
        }
        res.status(200);
        return "";
    }
}
