package org.example.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Date;
import java.util.Objects;

public class CargoDAL {

    private final String ID ="_id";
    private final String DESTINATION ="destination";
    private final String LOCATION ="location";
    private final String COURIER ="courier";
    private final String RECEIVED ="received";
    private final String STATUS ="status";
    private final String COLLECTION ="cargos";
    private final String DATABASE ="logistics";

    private final String IN_PROCESS = "in process";
    private final String DELIVERED = "delivered";


    private ObjectId id;
    private String destination;
    private String location;
    private String courier;
    private Date received;
    private String status;

    private String lastError;

    MongoClient mongoClient;
    MongoCollection<Document> cargoCollection;

    public ObjectId getId() {
        return id;
    }

    public String getDestination() {
        return destination;
    }

    public String getLocation() {
        return location;
    }

    public String getCourier() {
        return courier;
    }

    public Date getReceived() {
        return received;
    }

    public String getStatus() {
        return status;
    }

    public String getLastError() {
        return lastError;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public CargoDAL(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        this.cargoCollection = mongoClient.getDatabase(DATABASE).getCollection(COLLECTION);
    }

    public CargoDAL(ObjectId id, String destination, String location, String courier, Date received, String status, MongoClient mongoClient) {
        this.id = id;
        this.destination = destination;
        this.location = location;
        this.courier = courier;
        this.received = received;
        this.status = status;
        this.mongoClient = mongoClient;
    }

    public CargoDAL(ObjectId id,String destination, String location) {
        this.id = id;
        this.destination = destination;
        this.location = location;
    }

    public boolean isCityValid(String city) {
        WorldCitiesDAL worldCitiesDAL = new WorldCitiesDAL(this.getMongoClient());
        return worldCitiesDAL.isCityPresent(city);
    }

    public boolean isPlaneValid(String planeId) {
        PlanesDAL planesDAL = new PlanesDAL(this.getMongoClient());
        return planesDAL.isPlaneValid(planeId);
    }

    public boolean isPlaneLandedInLocation(String planeId,String location){
        PlanesDAL planesDAL = new PlanesDAL(this.getMongoClient());
        return planesDAL.isPlaneLandedInLocation(planeId,location);
    }

    Document createNewCargo(String location, String destination){

        if(!(isCityValid(location) && isCityValid(destination))) {
            this.lastError ="cities are not valid please check again";
            return null;
        }
        Document cargoDocument  = new Document(LOCATION,location).append(DESTINATION,destination);
        try {
            cargoCollection.insertOne(cargoDocument);
            return  cargoDocument;
        }
        catch (Exception me){
            this.lastError = me.toString();
            return null;
        }
    }


    Boolean cargoDelivered(String id){
        try {
            Bson filter = Filters.eq(ID,new ObjectId(id));
            Bson updates = new Document("$set",new Document(STATUS,DELIVERED).append(RECEIVED,new Date()));
            FindOneAndUpdateOptions findOneAndUpdateOptions = new FindOneAndUpdateOptions().upsert(false).returnDocument(ReturnDocument.AFTER);
            Document cargo = cargoCollection.findOneAndUpdate(filter, updates, findOneAndUpdateOptions);
            return !Objects.isNull(cargo);
        }
        catch (Exception me){
            this.lastError = me.toString();
            return null;
        }
    }

    Boolean cargoAssignCourier(String id,String planeId){
        if(!isPlaneValid(planeId)) {
            this.lastError = "plane doesn't exist please check ";
            return false;
        }
        try {
            Bson filter = Filters.eq(ID,new ObjectId(id));
            Bson updates = new Document("$set",new Document(COURIER,planeId));
            FindOneAndUpdateOptions findOneAndUpdateOptions = new FindOneAndUpdateOptions().upsert(false).returnDocument(ReturnDocument.AFTER);
            Document cargo = cargoCollection.findOneAndUpdate(filter, updates, findOneAndUpdateOptions);
            return !Objects.isNull(cargo);
        }
        catch (Exception me){
            this.lastError = me.toString();
            return null;
        }
    }

    Boolean cargoUnsetCourier(String id){
        try {
            Bson filter = Filters.eq(ID,new ObjectId(id));
            Bson updates = new Document("$set",new Document(COURIER,null));
            FindOneAndUpdateOptions findOneAndUpdateOptions = new FindOneAndUpdateOptions().upsert(false).returnDocument(ReturnDocument.AFTER);
            Document cargo = cargoCollection.findOneAndUpdate(filter, updates, findOneAndUpdateOptions);
            return !Objects.isNull(cargo);
        }
        catch (Exception me){
            this.lastError = me.toString();
            return null;
        }
    }

    Boolean cargoMove(String id,String location){
        try {
            Bson filter = Filters.eq(ID,new ObjectId(id));
            Document cargo = cargoCollection.find(filter).first();
            if(Objects.isNull(cargo)){
                this.lastError = "cargo not found ";
                return false;
            }
            if( isCityValid(location.trim())){
                if(isPlaneLandedInLocation(cargo.getString(COURIER),location)) {
                    Bson updates = new Document("$set", new Document(LOCATION, location));
                    FindOneAndUpdateOptions findOneAndUpdateOptions = new FindOneAndUpdateOptions().upsert(false).returnDocument(ReturnDocument.AFTER);
                    Document cargoUpdated = cargoCollection.findOneAndUpdate(filter, updates, findOneAndUpdateOptions);
                    return !Objects.isNull(cargoUpdated);
                }
                else{
                    this.lastError = "invalid city plane not yet landed in the given location";
                    return false;
                }
            }
            else if(isPlaneValid(location.trim())){
                if(isPlaneLandedInLocation(location.trim(),cargo.getString(LOCATION))) {
                    Bson updates = new Document("$set", new Document(LOCATION, location));
                    FindOneAndUpdateOptions findOneAndUpdateOptions = new FindOneAndUpdateOptions().upsert(false).returnDocument(ReturnDocument.AFTER);
                    Document cargoUpdated = cargoCollection.findOneAndUpdate(filter, updates, findOneAndUpdateOptions);
                    return !Objects.isNull(cargoUpdated);
                }
                else{
                    this.lastError = "invalid courier for cargo plane not yet landed in the given location";
                    return false;
                }
            }
            else{
                this.lastError = "invalid courier and city parametes please check again ";
                return false;
            }
        }
        catch (Exception me){
            this.lastError = me.toString();
            return false;
        }
    }

    ArrayList<Document> getCargoByLocation(String location){
        if(Objects.isNull(cargoCollection) || cargoCollection.countDocuments()  == 0){
            this.lastError = "empty cargo collection";
            return null;
        }
        Bson filter = Filters.and(Filters.eq(LOCATION,location),Filters.ne(STATUS,DELIVERED));
        ArrayList<Document> cargos = new ArrayList<Document>();
        try(MongoCursor<Document> cursor = cargoCollection.find(filter).cursor()) {
            while (cursor.hasNext()){
                Document c = cursor.next();
                cargos.add(c.append(ID,c.getObjectId(ID).toHexString()));
            }
        }
        return cargos;

    }

}
