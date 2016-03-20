import phantom from 'phantom';

class AmazonUtils {

  constructor() {
    this.loginSteps = [
      //Step 1 - Open Amazon home page
      function() {
        this.page.open("https://developer.amazon.com/edw/home.html#/skill/amzn1.echo-sdk-ams.app.f705b0c1-42a2-4c94-9e48-50aed83b2310/testing", function(status){ });
      },

      //Step 2 - Populate and submit the login form
      function() {
        this.page.evaluate(function(){
          document.getElementById("ap_email").value="gabriel_lyons@brown.edu";
          document.getElementById("ap_password").value="vinhsucks";
          document.getElementById("ap_signin_form").submit();
        });
      },

      //Step 3 - Go to test page
      function() {
        this.page.evaluate(function(){
          window.location.replace("https://developer.amazon.com/edw/home.html#/skill/amzn1.echo-sdk-ams.app.f705b0c1-42a2-4c94-9e48-50aed83b2310/testing");
        });
      },

    ];
  }

  generateSubmitSteps(query) {
    return [
      //Step 1 - enter your message
      function() {
        this.page.evaluate(function(query){
          document.getElementById("edw-test-utteranceTextField").value=query;
        });
      },

      //Step 2 - submit the message
      function() {
        this.page.evaluate(function(){
          document.getElementById("edw-test-textAskButton").click();
        });
      },
    ];
  }

  login() {
    phantom.create(function (ph) {
      ph.createPage(function (page) { 
        this.page = page;
        this.phatom = ph;
        this.page = webPage.create();
        this.page.settings.userAgent = 'Mozilla/5.0 (Windows NT 10.0; WOW64) ' +
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 ' +
            'Safari/537.36';
        this.page.settings.javascriptEnabled = true;
        this.page.settings.loadImages = false;
        this.phantom.cookiesEnabled = true;
        this.phantom.javascriptEnabled = true;
        this.page.onLoadStarted = function() {
          this.loadInProgress = true;
          console.log('Loading started');
        };
        this.page.onLoadFinished = function() {
          this.loadInProgress = false;
          console.log('Loading finished');
        };
        this.page.onConsoleMessage = function(msg) {
          console.log(msg);
        };
        this.testIndex = 0;
        this.loginInterval = setInterval(stepByStep.bind(this, this.loginSteps), 50);
      });
    });
  }

  submitText(query) {
    const steps = this.generateSubmitSteps(query);
    this.submitInterval = setInterval(stepByStep.bind(this, this.steps), 50);
  }

  stepByStep(steps) {
    if (this.loadInProgress == false && typeof steps[this.testindex] == "function") {
        steps[this.testindex]();
        this.testindex++;
    }
    if (typeof steps[this.testindex] != "function") {
        console.log("test complete!");
    }
  }

}

export default new AmazonUtils();
