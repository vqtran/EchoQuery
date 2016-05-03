import alt from '../alt';

class DisplayActions {
  setDisplayData(data) {
    return JSON.parse(data);
  }
  setMode(data) {
    return data;
  }
  setColumns(data) {
    return data;
  }
}

export default alt.createActions(DisplayActions);
