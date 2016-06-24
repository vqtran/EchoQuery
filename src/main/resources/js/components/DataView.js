import React from 'react';

import DataTable from '../components/DataTable.js';
import HeatMap from '../components/HeatMap.js';
import SessionStore from '../stores/SessionStore';
import WindowStore from '../stores/WindowStore';

class DataView extends React.Component {
  constructor(props) {
    super(props);
    this.onChange = this.onChange.bind(this);
    this.state = {...WindowStore.getState(), ...SessionStore.getState()}
  }

  render() {
    if (this.state.chosenColumns.length == 1 && this.state.chosenColumns[0] == "") {
      return <DataTable />;
    } else {
      return <HeatMap />;
    }
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

export default DataView;
