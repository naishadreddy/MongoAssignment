package org.example.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Objects;

public class PlaneMaintenanceDAL {

    private final String ID="_id";
    private final String MAINTENANCE_DUE_DATE="maintenanceDueDate";
    private final String STATUS="status";
    private final String DISTANCE="distanceTravelledInMiles";
    private final String DATABASE ="logistics";
    private final String COLLECTION ="planeMaintenance";
    private final String SERVICED ="serviced";
    private final String PENDING ="pending";

    Logger logger;
    private String id;
    private Date maintenanceDueDate;
    private Double status;
    private Double distanceTravelledInMiles;

    private String lastError;
    private MongoClient mongoClient;
    private MongoCollection planeMaintenanceCollection;

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public PlaneMaintenanceDAL(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        this.planeMaintenanceCollection = mongoClient.getDatabase(DATABASE).getCollection(COLLECTION);
        logger = LoggerFactory.getLogger(PlaneMaintenanceDAL.class);
    }

    public PlaneMaintenanceDAL(String id,Date maintenanceDueDate, Double status, Double distanceTravelledInMiles, MongoClient mongoClient) {
        this.id = id;
        this.maintenanceDueDate = maintenanceDueDate;
        this.status = status;
        this.distanceTravelledInMiles = distanceTravelledInMiles;
        this.mongoClient = mongoClient;
        this.planeMaintenanceCollection = mongoClient.getDatabase(DATABASE).getCollection(COLLECTION);
    }

    Boolean addPlaneRecord(String planeId,double distance){
        try {
            Document OldDoc = new Document(ID,planeId);
            Document planeDoc = new Document("$set",new Document(DISTANCE, distance))
                                                .append("$setOnInsert",new Document(MAINTENANCE_DUE_DATE, new Date())
                                                                            .append(STATUS,PENDING));
            FindOneAndUpdateOptions findOneAndUpdateOptions = new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER);
            Document planeMUpdated = (Document) planeMaintenanceCollection.findOneAndUpdate(OldDoc, planeDoc, findOneAndUpdateOptions);
            return !Objects.isNull(planeMUpdated);
        }
        catch(Exception e){
            logger.error(" error creating document "+ e);
            this.lastError = e.toString();
            return false;
        }
    }

}
