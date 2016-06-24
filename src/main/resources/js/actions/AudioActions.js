import alt from '../alt';

class AudioActions {
  setTranscript(transcript) {
    return transcript;
  }

  appendTranscript(transcript) {
    return transcript;
  }

  setTemp(transcript) {
    return transcript;
  }

  appendTemp(transcript) {
    return transcript;
  }

  toggleRecord() {
    return {};
  }

}

export default alt.createActions(AudioActions);
