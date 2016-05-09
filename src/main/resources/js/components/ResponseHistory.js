import React from 'react';

import WindowStore from '../stores/WindowStore';
import SessionStore from '../stores/SessionStore';

class ResponseHistory extends React.Component {

  constructor(props) {
    super(props);
    this.onChange = this.onChange.bind(this);
    this.generateRows = this.generateRows.bind(this);
    this.state = {...WindowStore.getState(), ...SessionStore.getState()}
  }

  render() {
    return (
      <ul className="response-history list-group">
        {this.generateRows()}
      </ul>
    );
  }

  generateRows() {
    const ret = [];
    let isFirst = true;
    for (let i = 0; i < this.state.responseHistory.length; i++) {
      ret.push(<li 
        className={isFirst ? "list-group-item active" : "list-group-item"}>
          {this.state.responseHistory[i]}
        </li>
      );
      isFirst = false;
    }
    return ret;
  }

  componentDidMount() {
    WindowStore.listen(this.onChange);
    SessionStore.listen(this.onChange);
  }

  componentWillUnmount() {
    WindowStore.unlisten(this.onChange);
    SessionStore.unlisten(this.onChange);
  }

  onChange(state) {
    this.setState(state);
  }

}

export default ResponseHistory;
