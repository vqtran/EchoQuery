import alt from '../alt';

import WindowActions from '../actions/WindowActions';

class WindowStore {
  constructor() {
    this.bindListeners({
      updateWindow: WindowActions.setWindow,
      updateVizWidth: WindowActions.setVizWidth,
    });

    this.state = {
      width: 1000,
      height: 1000,
      vizWidth: 0,
    };

    this.exportPublicMethods({
      getWindow: this.getWindow,      
    });
  }

  updateWindow(data) {
    this.setState({
      width: data.width,
      height: data.height,
    });
  }

  updateVizWidth(data) {
    this.setState({
      vizWidth: data,
    });
  }

  getWindow() {
    return {
      width: this.state.width, 
      height: this.state.height, 
    }
  }
}

export default alt.createStore(WindowStore);
