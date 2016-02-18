import React from 'react';

import SessionActions from '../actions/SessionActions';
import SessionStore from '../stores/SessionStore';

class UserIdInput extends React.Component {

  constructor(props) {
    super(props);
    this.handleChange = this.handleChange.bind(this);
    this.onChange = this.onChange.bind(this);
    this.state = SessionStore.getState();
  }

  handleChange(evt) {
    SessionActions.setSessionId(evt.target.value);
  }

  componentDidMount() {
    SessionStore.listen(this.onChange);
  }

  componentWillUnmount() {
    SessionStore.unlisten(this.onChange);
  }

  onChange(state) {
    this.setState(state);
  }

  render () {
    var value = this.state.userId;
    return <input type="text" value={value} onChange={this.handleChange} />;
  }
}

export default UserIdInput;
