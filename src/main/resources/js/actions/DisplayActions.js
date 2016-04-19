import alt from '../alt';

class DisplayActions {
  setDisplayData(data) {
    return JSON.parse(data);
  }
}

export default alt.createActions(DisplayActions);
