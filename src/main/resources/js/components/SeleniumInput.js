import React from 'react';
import $ from "jquery";

class SeleniumInput extends React.Component {
  constructor(props) {
    super(props);
    fetch('/login/id_placeholder_here');
  }

  render() {
    return (
      <input type="text" id="selenium-input" className="form-control"
        placeholder="What do you want?" onKeyDown={this.handleKeyEvent} />
    );
  }

  handleKeyEvent(event) {
    if(event.keyCode == 13) {
      fetch('/submit/id_placeholder_here', {
        method: 'post',
        body: $("#selenium-input").val(),
      });
      $("#selenium-input").val("");
    }
  }
}

export default SeleniumInput;
