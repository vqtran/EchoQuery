import React from 'react';
import FixedDataTable from 'fixed-data-table';

const Table = FixedDataTable.Table
const Column = FixedDataTable.Column
const Cell = FixedDataTable.Cell

import WindowStore from '../stores/WindowStore';
import SessionStore from '../stores/SessionStore';

class DataTable extends React.Component {

  constructor(props) {
    super(props);
    this.onChange = this.onChange.bind(this);
    this.generateColumns = this.generateColumns.bind(this);
    this.state = {...WindowStore.getState(), ...SessionStore.getState()}
  }

  render() {
    return (
      <div className="data-table">
        <Table
          headerHeight={50}
          rowsCount={this.getRowsCount()}
          rowHeight={50}
          width={this.state.vizWidth}
          height={this.state.height-80}>
          {this.generateColumns()}
        </Table>
      </div>
    );
  }

  getRowsCount() {
    let maximum = 1;
    for (const propt in this.state.displayData) {
      maximum = Math.max(maximum, this.state.displayData[propt].length);
    }
    return maximum;
  }

  generateColumns() {
    const ret = [];
    for (const propt in this.state.displayData) {
      ret.push(<Column 
          key={propt}
          header={<Cell>{propt}</Cell>}
          cell={props => (
            <Cell {...props}>
              {this.state.displayData[propt][props.rowIndex]}
            </Cell>
          )}
          width={100} 
          flexGrow={1}/>);
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

export default DataTable;
