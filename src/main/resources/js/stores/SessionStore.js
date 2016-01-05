import alt from '../alt';

import DisplayActions from '../actions/DisplayActions';
import SessionActions from '../actions/SessionActions';

class SessionStore {

  constructor() {
    this.bindListeners({
      updateDisplayText: DisplayActions.setDisplayText,
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

  updateDisplayText(data) {
    this.setState({
      displayText: data,
    });
  }

  updateSessionId(data) {
    this.setState({
      userId: data,
    });
  }

  getSessionId() {
    return this.state.userId;
  }
}

export default alt.createStore(SessionStore);
