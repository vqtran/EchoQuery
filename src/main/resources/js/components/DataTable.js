import React from 'react';
import FixedDataTable from 'fixed-data-table';

const Table = FixedDataTable.Table
const Column = FixedDataTable.Column
const Cell = FixedDataTable.Cell

import WindowStore from '../stores/WindowStore';
import SessionStore from '../stores/SessionStore';

const TextCell = ({rowIndex, data, col, ...props}) => (
  <Cell {...props}>
    {data.getObjectAt(rowIndex)[col]}
  </Cell>
);

class DataTable extends React.Component {

  constructor(props) {
    super(props);
    this.onChange = this.onChange.bind(this);
    this.generateColumns = this.generateColumns.bind(this);
    this.state = WindowStore.getState();
    this.setState(SessionStore.getState());
  }

  render() {
    return (
      <Table
        headerHeight={50}
        rowsCount={this.getRowsCount()}
        rowHeight={50}
        width={this.state.width}
        height={this.state.height-200}>
        {this.generateColumns()}
      </Table>
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
    console.log(this.state.displayData);
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
