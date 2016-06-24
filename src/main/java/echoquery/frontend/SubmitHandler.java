package echoquery.frontend;

import spark.Request;
import spark.Response;

public class SubmitHandler {
  Request request;

  public SubmitHandler(Request request, Response response) { 
    // get the users credentials using request.params(":id")
    SeleniumController.getInstance().submit(request.body());
  }

  public String respond() {
    return "SUCCESS";
  }
}