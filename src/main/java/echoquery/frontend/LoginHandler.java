package echoquery.frontend;

import spark.Request;
import spark.Response;

public class LoginHandler {

  public LoginHandler(Request request, Response response) { 
    System.out.println("a");
    SeleniumController.getInstance().login();
    System.out.println("b");
  }

  public String respond() {
    return "SUCCESS";
  }
}
