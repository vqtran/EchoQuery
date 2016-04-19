import alt from '../alt';

import DisplayActions from '../actions/DisplayActions';
import SessionActions from '../actions/SessionActions';

class SessionStore {

  constructor() {
    this.bindListeners({
      updateDisplayData: DisplayActions.setDisplayData,
      updateSessionId: SessionActions.setSessionId,
    });

    this.state = {
      displayText: "What do you want?",
      userId: 0,
    };

    this.exportPublicMethods({
      getSessionId: this.getSessionId,      
    });
  }

  updateDisplayData(data) {
    this.setState({
      displayText: data["sessions.display"],
      displayData: JSON.parse(data["sessions.result"]),
    });
  }

  updateSessionId(data) {
    if (!/[^a-zA-Z0-9]/.test(data)) {
      // input is alphanumeric
      this.setState({
        userId: data,
      });
    }
  }

  getSessionId() {
    return this.state.userId;
  }
}

export default alt.createStore(SessionStore);
