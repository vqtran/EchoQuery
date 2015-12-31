class SessionUtils {
  static giveSessionDisplayText(callback) {
    fetch('/fetch').then((response) => {return response.text();})
      .then((text) => {callback(text);});
  }
}

export default SessionUtils;
