import React from 'react';

import DataTable from '../components/DataTable.js';
import DisplayActions from '../actions/DisplayActions';
import SessionStore from '../stores/SessionStore';
import SessionUtils from '../utils/SessionUtils';
import UserIdInput from '../components/UserIdInput.js';
import WindowActions from '../actions/WindowActions.js';

class App extends React.Component {

  render() {
    return (
      <div>
        <UserIdInput />
        <div className="text-center">
          <h1>{this.state.displayText}</h1>
          <DataTable />
        </div>
      </div>
    );
  }

  componentDidMount() {
    window.addEventListener("resize", this.updateDimensions);
    SessionStore.listen(this.onChange);
    this.timer = setInterval(() => {
      SessionUtils.giveSessionDisplayData(DisplayActions.setDisplayData);
    }, 250);
  }

  componentWillUnmount() {
    window.removeEventListener("resize", this.updateDimensions);
    clearInterval(this.timer);
    SessionStore.unlisten(this.onChange);
  }

  componentWillMount() {
    this.updateDimensions();
  }

  updateDimensions() {
    const w = window,
      d = document,
      documentElement = d.documentElement,
      body = d.getElementsByTagName('body')[0],
      width = w.innerWidth || documentElement.clientWidth || body.clientWidth,
      height = w.innerHeight|| documentElement.clientHeight|| body.clientHeight;

      WindowActions.setWindow({width: width, height: height});
  }

  onChange(state) {
    this.setState(state);
  }

  constructor(props) {
    super(props);
    this.onChange = this.onChange.bind(this);
    this.state = SessionStore.getState();
  }

}

export default App;
