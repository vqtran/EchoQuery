package echoquery.frontend;

import echoquery.utils.VisualizationUtil;
import spark.Request;
import spark.Response;

public class UpdateHandler {
  public UpdateHandler(Request request, Response response) {
    VisualizationUtil.updateDisplayText(request.queryParams("display"), null);
  }

  public String respond() {
    return "";
  }
}
