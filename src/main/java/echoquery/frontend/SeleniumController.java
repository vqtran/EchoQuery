package echoquery.frontend;

import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

public class SeleniumController {
  private static final SeleniumController instance = new SeleniumController();
  private ChromeDriver driver;
  
  private SeleniumController() { 
    this.driver = new ChromeDriver();
  }
  
  public void login() {
    driver.get("https://developer.amazon.com/login.html");
    WebElement emailField=driver.findElement(By.id("ap_email"));
    emailField.sendKeys("gabriel_lyons@brown.edu");
    WebElement passwordField=driver.findElement(By.id("ap_password"));
    passwordField.sendKeys("NOT_FOR_PUBLIC_CONSUMPTION");
    WebElement signinButton=driver.findElement(By.id("signInSubmit-input"));
    signinButton.click();
    driver.get("https://developer.amazon.com/edw/home.html#/skill/amzn1.echo-sdk-ams.app.f705b0c1-42a2-4c94-9e48-50aed83b2310/testing");
  }
  
  public void submit(String query) {
    try {
      WebElement queryField=driver.findElement(By.id("edw-test-utteranceTextField"));
      queryField.clear();
      queryField.sendKeys(query);
      WebElement submitButton=driver.findElement(By.id("edw-test-textAskButton"));
      submitButton.click();
    } catch (Throwable e) {
      System.out.println("throwable");
    }
  }
  
  public static SeleniumController getInstance() {
    return instance;
  }
}
