package echoquery.frontend;

import echoquery.utils.VisualizationUtil;
import spark.Request;
import spark.Response;

public class UpdateHandler {
  public UpdateHandler(Request request, Response response) {
    VisualizationUtil.updateDisplayText(request.queryParams("display"));
  }

  public String respond() {
    return "";
  }
}
