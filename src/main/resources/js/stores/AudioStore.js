import alt from '../alt';

import AudioActions from '../actions/AudioActions';

class AudioStore {
  constructor() {
    this.bindListeners({
      updateTranscript: AudioActions.setTranscript,
      appendTranscript: AudioActions.appendTranscript,
      updateTemp: AudioActions.setTemp,
      appendTemp: AudioActions.appendTemp,
      toggleRecord: AudioActions.toggleRecord,
    });

    this.state = {
      transcript: "Speak Now.",
      temp: "",
      recording: false,
    };
  }

  toggleRecord(data) {
    this.setState({
      recording: !this.state.recording,
    });
  }

  updateTranscript(transcript) {
    this.setState({
      transcript: transcript,
    });
  }

  appendTranscript(transcript) {
    this.setState({
      transcript: this.state.transcript + transcript,
    });
  }

  updateTemp(temp) {
    this.setState({
      temp: temp,
    });
  }

  appendTemp(temp) {
    this.setState({
      temp: this.state.temp + temp,
    });
  }

  getTranscript() {
    return this.state.transcript;
  }

  getTemp() {
    return this.state.temp;
  }
}

export default alt.createStore(AudioStore);
