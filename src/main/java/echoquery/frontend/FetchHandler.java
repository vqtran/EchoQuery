package echoquery.frontend;

import echoquery.utils.VisualizationUtil;
import spark.Request;
import spark.Response;

public class FetchHandler {
  Request request;

  public FetchHandler(Request request, Response response) {
    this.request = request;
  }

  public String respond() {
    return VisualizationUtil.getDisplayText(request.params(":id"));
  }
}