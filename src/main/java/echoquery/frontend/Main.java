package echoquery.frontend;

import java.util.HashMap;

import spark.ModelAndView;
import spark.Spark;
import spark.template.jade.JadeTemplateEngine;

public class Main {

  public static void main(String[] args) {
    runSparkServer();
  }

  private static void runSparkServer() {
    Spark.externalStaticFileLocation("public/");

    Spark.get("/",
        (request, response) -> new ModelAndView(new HashMap<>(), "index.jade"),
        new JadeTemplateEngine());

    Spark.get("/test", (request, response) -> "Hello World!");
    
    Spark.get("/fetch", (request, response) -> 
        new FetchHandler(request, response).respond());
  }
}