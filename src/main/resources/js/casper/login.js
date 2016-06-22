var casper = require('casper').create({
  verbose: true,
  logLevel: "debug",
  pageSettings: {
    userAgent: 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)',
  }
});

var fs = require('fs');

casper.start().thenOpen("https://developer.amazon.com/login.html", function(){ });

casper.waitForSelector("#ap_email", function() { }, 
  function then() {}, function timeout() { }, 20000);
casper.waitForSelector("#ap_password", function() { }, 
  function then() {}, function timeout() { }, 20000);
casper.waitForSelector("#signInSubmit", function() { }, 
  function then() {}, function timeout() { }, 20000);

casper.then(function(){
  this.evaluate(function insertingText(){
    document.getElementById("ap_email").value="gabriel_lyons@brown.edu";
    document.getElementById("ap_password").value="vinhsucks";
  });
});

casper.wait(1000, function() {
    this.echo("I've waited for a second.");
});

casper.then(function(){
    console.log("Make a screenshot and save it as Before.png");
    this.capture('BeforeLogin.png');
});

casper.then(function clickingSubmit(){
  this.evaluate(function(){
    document.getElementById("signInSubmit-input").click();
  });
});

casper.wait(1000, function() {
    this.echo("I've waited for 1 second.");
});

casper.then(function(){
    console.log("Make a screenshot and save it as AfterLogin.png");
    this.capture('AfterLogin.png');
    var result = this.evaluate(function() {
      return document.querySelectorAll("html")[0].outerHTML;
    });
    fs.write('AmazonLoggedIn.html',result,'w');
});

casper.then(function() {
  this.evaluate(function(){
    document.getElementById("edw-test-utteranceTextField").value="how many customers are there";
  });
});

casper.waitForSelector("#edw-test-textAskButton", function() { }, 
  function then() {}, function timeout() { }, 20000);

casper.then(function() {
  this.evaluate(function(){
    document.getElementById("edw-test-textAskButton").click();
  });
});

casper.run();
