import React from 'react';

import DataView from '../components/DataView';
import ResponseHistory from '../components/ResponseHistory';
import AudioInput from '../components/AudioInput';
import DisplayActions from '../actions/DisplayActions';
import SessionStore from '../stores/SessionStore';
import SessionUtils from '../utils/SessionUtils';
import SeleniumInput from '../components/SeleniumInput';
import WindowActions from '../actions/WindowActions';
import $ from "jquery";

class App extends React.Component {

  render() {
    return (
      <div>
        <div className="text-center container-fluid">
          <div className="col-md-3">
            <img src="http://localhost:4567/images/logo.png" className="logo"/>
            <SeleniumInput />
            <ResponseHistory />
            <img src="http://localhost:4567/images/schema.png" className="schema"/>
          </div>
          <div id="vizHolder" className="col-md-9">
            <DataView />
          </div>
        </div>
      </div>
    );
  }

  componentDidMount() {
    this.updateDimensions();
    window.addEventListener("resize", this.updateDimensions);
    SessionStore.listen(this.onChange);
    this.timer = setInterval(() => {
      SessionUtils.giveSessionDisplayData(DisplayActions.setDisplayData);
    }, 1000);
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
    if($("#vizHolder").length != 0) {
      WindowActions.setVizWidth($("#vizHolder").width());
    }
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
