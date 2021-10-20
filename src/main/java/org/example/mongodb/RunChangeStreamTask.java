package org.example.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RunChangeStreamTask implements Runnable {

    Logger logger;
    int threadId;
    MongoClient mongoClient;


    RunChangeStreamTask(int threadid, MongoClient mongoClient) {
        logger = LoggerFactory.getLogger(RunChangeStreamTask.class);
        this.threadId = threadid;
        this.mongoClient = mongoClient;
    }

 

    public void run() {
        logger.info("Thread {} has started.", threadId);
        PlanesDAL planesDAL = new PlanesDAL(mongoClient);
        MongoCursor<ChangeStreamDocument<Document>> cursor = openChangeStream(planesDAL);
        while(cursor.hasNext()){
            ChangeStreamDocument cdBson = cursor.next();
            if(cdBson.getUpdateDescription().getUpdatedFields().containsKey("landed")){
                logger.info("updating duration and distance covered for "+cdBson.getDocumentKey().getString("_id").getValue());
                planesDAL.updateDistanceAndTime(cdBson.getDocumentKey().getString("_id").getValue());
            }
        }
    }

    MongoCursor<ChangeStreamDocument<Document>> openChangeStream(PlanesDAL planesDAL){

        MongoCollection planesCollection = planesDAL.getPlanesCollection();
        Bson filterStage = Aggregates.match(Filters.and(Filters.eq("operationType","update")));

        List<Bson> pipeline = Collections.singletonList(filterStage);
        //System.out.println(" change event captured "+cursor.next());
        return (MongoCursor<ChangeStreamDocument<Document>>) planesCollection.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP).iterator();
    }


}