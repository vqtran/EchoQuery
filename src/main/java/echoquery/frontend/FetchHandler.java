package echoquery.frontend;

import echoquery.utils.VisualizationUtil;
import spark.Request;
import spark.Response;

public class FetchHandler {
  
  public FetchHandler(Request request, Response response) { }

  public String respond() {
    return VisualizationUtil.getDisplayText();
  }
}
