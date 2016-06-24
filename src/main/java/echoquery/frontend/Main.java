package echoquery.frontend;

import java.util.HashMap;

import echoquery.utils.VisualizationUtil;
import spark.ModelAndView;
import spark.Spark;
import spark.template.jade.JadeTemplateEngine;

public class Main {

  public static void main(String[] args) {
    runSparkServer();
  }

  private static void runSparkServer() {
    Spark.externalStaticFileLocation("public/");

    Spark.get("/user/:id", (request, response) ->
        new ModelAndView(new HashMap<>(), "index.jade"),
        new JadeTemplateEngine());

    Spark.get("/fetch/:id", (request, response) ->
        VisualizationUtil.getUserData(request.params(":id")).toString());

    Spark.get("/fetch/", (request, response) -> "Enter a valid User ID");
    Spark.get("/login/:id", (request, response) -> 
        new LoginHandler(request, response).respond());
    Spark.post("/submit/:id", (request, response) -> 
        new SubmitHandler(request, response).respond());
  }
}
