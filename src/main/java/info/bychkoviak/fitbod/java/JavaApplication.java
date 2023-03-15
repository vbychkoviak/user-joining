package info.bychkoviak.fitbod.java;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class JavaApplication {

  public static void main(String[] args) throws IOException {
    String usersPath = "users.csv";
    String aliasPath = "alias.csv";
    String eventsPath = "events.csv";
    String outputPath = "output.csv";
    
    Map<String, String> users = readFile(usersPath, 0, 1);
    Map<String, String> aliases = readFile(aliasPath, 2 ,1);
    
    int lines = 0;
    
    try (BufferedReader br = new BufferedReader(new FileReader(eventsPath));
        PrintWriter pw = new PrintWriter(outputPath)){
      br.readLine(); //skip headers
      pw.println("user_email,feature_key,feature_value");
      
      String line; 
      
      while ((line = br.readLine())!=null) {
        String[] split = line.split(",");
        String user = split[1];
        
        while (!users.containsKey(user)) {
          String alias = user;
          user  = aliases.get(alias);
          if (user==null) {
            throw new IllegalStateException("can't find user for alias: "+alias);
          }
        }
        
        String userEmail = users.get(user);
        pw.println(userEmail+","+split[2]+","+split[3]);
        lines++;
        
      }
    }
    
    System.out.println(lines+" lines saved");
    
  }
  
  public static Map<String,String> readFile(String path, int keyColumn, int valueColumn) throws IOException {
    Map<String, String> result = new HashMap<String, String>();
    try (BufferedReader br = new BufferedReader(new FileReader(path))){
      String line;
      
      br.readLine(); // skip headers
      while ((line = br.readLine())!=null) {
        String[] split = line.split(",");
        String key = split[keyColumn];
        String value = split[valueColumn];
        
        result.put(key, value);
      }
    }
    
    return result;
  }

}
