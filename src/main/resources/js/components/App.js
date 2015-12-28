import React from 'react';

import DisplayActions from '../actions/DisplayActions';
import SessionStore from '../stores/SessionStore';
import SessionUtils from '../utils/SessionUtils';

class App extends React.Component {

  render() {
    return (
      <div>
        {this.state.displayText}
      </div>
    );
  }

  componentDidMount() {
    SessionStore.listen(this.onChange);
    this.timer = setInterval(() => {
      SessionUtils.giveSessionDisplayText(DisplayActions.setDisplayText);
    }, 50);
  }

  componentWillUnmount() {
    clearInterval(this.timer);
    SessionStore.unlisten(this.onChange);
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