# Description
This is take home assignment for joining users to there aliases.

# Requirements
`java` and `maven` should be installed. 
solution has been tested with java 8. 

both pure java and Spark implementations are expecting following files in the project root folder:
 - `users.csv` (user_id,email)
 - `alias.csv` (timestamp,user_id,alias_user_id)
 - `events.csv` timestamp,user_id,feature_key,feature_value

pure java implementation will produce `output.csv` file as a output
spark implementation will produce CSV file in the folder `output` (it will fail if folder already exists)

# Building jar
```bash
mvn clean package
```

# Running Java application 
```
java -cp target/assignment-0.0.1-SNAPSHOT-jar-with-dependencies.jar info.bychkoviak.fitbod.java.JavaApplicationj
```

# Running Spark application
```
java -cp target/assignment-0.0.1-SNAPSHOT-jar-with-dependencies.jar info.bychkoviak.fitbod.spark.SparkApplication
```
