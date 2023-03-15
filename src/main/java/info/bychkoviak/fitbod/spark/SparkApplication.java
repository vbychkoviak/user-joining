package info.bychkoviak.fitbod.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkApplication {

  public static void main(String[] args) {
    
    SparkSession spark = SparkSession.builder().master("local[*]").appName("users alias feature query").getOrCreate();
    
    Dataset<Row> users = spark.read().option("header","true").csv("users.csv");
    Dataset<Row> alias = spark.read().option("header","true").csv("alias.csv");
    Dataset<Row> events = spark.read().option("header","true").csv("events.csv");
    
    Dataset<Row> allusers = users;
    
    while (true) {
      users = alias.join(users, "user_id").select(alias.col("alias_user_id"), users.col("email")).withColumnRenamed("alias_user_id", "user_id");
      allusers = allusers.union(users);
      
      if (users.count()==0) {
        break;
      }
    }
    Dataset<Row> userFeature = events.join(allusers,"user_id").select(allusers.col("email"), events.col("feature_key"), events.col("feature_value")).withColumnRenamed("email", "user_email");
    userFeature.coalesce(1).write().option("header","true").csv("output");
      
  }

}
