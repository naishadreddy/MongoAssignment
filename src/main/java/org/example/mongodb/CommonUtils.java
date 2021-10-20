package org.example.mongodb;

import java.net.ConnectException;
import java.time.Duration;

import com.mongodb.MongoException;
import net.jodah.failsafe.RetryPolicy;

public class CommonUtils {

    public static RetryPolicy<Object> retryMethod() {
         return new RetryPolicy<>().handle(MongoException.class)
                .withDelay(Duration.ofSeconds(10))
                .withMaxRetries(2);
    }



}
