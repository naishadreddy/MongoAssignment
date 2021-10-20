
## Getting Started
- Install [Java 8+](https://www.oracle.com/technetwork/java/javase/downloads/index-jsp-138363.html)
- Install [Maven](https://www.scala-sbt.org/) the build tool for Java projects
- Clone the git repository

## Build and Test
1. Local build and test
- `cd {PROJECT ROOT}`
- Run `mvn clean compile` to compile 

2. Building an uberjar (single Jar file containing all dependencies for ease of deployment)
- Run `mvn package` from project root
- This will create an uberjar file in `{PROJECT_ROOT}/webService.jar`

## How to Execute
 
- Move the UI code from static folder to /home/usr/static
- start the webService using `java jar webService.jar  `

## Configuration
- By default, all the web services run on port 5000 (can be changed in WebService.java).
- Code contains four DAL Classes read and update data from planes, cities and cargoes collection.
- All collections are created under logistics database.  
- During startup of service code creates all necessary indices if they don't exist.  
- Background ChangeStream cursor runs in background to update the distance travelled and time taken to cover the distance.
- planes whose mileage is more than 50000 miles will be logged into planeMaintenance collection of logistics database.

## Retries 
- Failsafe package has been used to retry updates to collections only when an mongoException occurs.

## logging 
- s14j logging module is configured in all DAL classes.
- modify /src/resources/logback.xml to change logging format and destination.









