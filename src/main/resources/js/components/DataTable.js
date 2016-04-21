import React from 'react';
import FixedDataTable from 'fixed-data-table';

const Table = FixedDataTable.Table
const Column = FixedDataTable.Column
const Cell = FixedDataTable.Cell

import WindowStore from '../stores/WindowStore';

class DataTable extends React.Component {

  constructor(props) {
    super(props);
    this.onChange = this.onChange.bind(this);
    this.state = WindowStore.getState();
  }

  render() {
    return (
      <Table
        rowsCount={100}
        rowHeight={50}
        width={this.state.width}
        height={this.state.height-200}>
        <Column cell={<Cell>Basic content</Cell>} width={100} flexGrow={1}/>
        <Column cell={<Cell>Advnced Content</Cell>} width={100} flexGrow={1}/>
      </Table>
    );
  }

  componentDidMount() {
    WindowStore.listen(this.onChange);
  }

  componentWillUnmount() {
    WindowStore.unlisten(this.onChange);
  }

  onChange(state) {
    this.setState(state);
  }

}

export default DataTable;
