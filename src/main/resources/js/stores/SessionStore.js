import alt from '../alt';
import DisplayActions from '../actions/DisplayActions';

class SessionStore {

  constructor() {
    this.bindListeners({
      updateDisplayText: DisplayActions.setDisplayText,
    });

    this.state = {
      displayText: "What do you want?",
    };
  }

  updateDisplayText(data) {
    this.setState({
      displayText: data,
    });
  }
}

export default alt.createStore(SessionStore);
