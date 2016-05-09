import SessionStore from '../stores/SessionStore';

class SessionUtils {
  static giveSessionDisplayData(callback) {
    fetch('/fetch/' + SessionStore.getSessionId()).then((response) => 
      {return response.text();})
      .then((text) => {callback(text);});
  }
}

export default SessionUtils;
