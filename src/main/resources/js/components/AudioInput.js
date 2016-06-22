import React from 'react';

import AudioActions from '../actions/AudioActions';
import AudioStore from '../stores/AudioStore';

class AudioInput extends React.Component {
  constructor(props) {
    super(props);
    this.startSpeech = this.startSpeech.bind(this);
    this.state = AudioStore.getState();
    this.onChange = this.onChange.bind(this);
    this.submitSpeech = this.submitSpeech.bind(this);

    if (!('webkitSpeechRecognition' in window)) {
      alert("you need to upgrade your browser!");
    } else {
      this.recognition = new webkitSpeechRecognition();
      this.recognition.continuous = true;
      this.recognition.interimResults = true;

      this.recognition.onstart = function() { /* do nothing */ }
      this.recognition.onresult = this.handleSpeechResult; 
      this.recognition.onerror = function(event) { /* do nothing */ }
      this.recognition.onend = function() { /* do nothing */ } 
      AudioActions.setTranscript('Speak Now');
    }

    fetch('/login/id_placeholder_here');
  }

  render() {
    return (
      <div className="speech-input">
        <h2>{this.state.transcript}</h2>
        <h2>{this.state.temp}</h2>
        <button onClick={this.startSpeech}>
          {this.state.recording ? "Stop" : "Record"}
        </button>
        {this.state.recording ? "" : 
          <button onClick={this.submitSpeech}>Submit</button>} 
      </div>
    );
  }

  submitSpeech() {
    fetch('/submit/id_placeholder_here', {
      method: 'post',
      body: this.state.transcript
    });
  }

  startSpeech() {
    if (this.state.recording) {
      this.recognition.stop();
      
    } else {
      AudioActions.setTranscript('');
      AudioActions.setTemp('');
      this.recognition.start();
    }
    AudioActions.toggleRecord();
  }

  handleSpeechResult(event) {
    AudioActions.setTemp('');
    for (var i = event.resultIndex; i < event.results.length; ++i) {
      if (event.results[i].isFinal) {
        AudioActions.appendTranscript(event.results[i][0].transcript);
      } else {
        AudioActions.appendTemp(event.results[i][0].transcript);
      }
    }
  }

  componentDidMount() {
    AudioStore.listen(this.onChange);
  }

  componentWillUnmount() {
    AudioStore.unlisten(this.onChange);
  }

  onChange(state) {
    this.setState(state);
  }
}

export default AudioInput;
