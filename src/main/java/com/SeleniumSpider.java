package com;

import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

import java.sql.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SeleniumSpider {
    public String TARGET_URL = "https://m.weibo.cn/u/";
    public String UID = "6284091238";
    public String FINAL_TARGET_URL = TARGET_URL + UID;

    public SeleniumSpider(String uid) {
        UID = uid;
        FINAL_TARGET_URL = TARGET_URL + UID;
    }

    public void run() throws InterruptedException, ClassNotFoundException, SQLException {
        System.setProperty("webdriver.chrome.driver", "/usr/local/share/chromedriver");
        WebDriver driver = new ChromeDriver();
        driver.get(FINAL_TARGET_URL);

        Thread.sleep(1500);

        // WebElement webElement = driver.findElement(By.cssSelector(".item-list"));
        // 关注数
        WebElement webElement_follow = driver.findElement(By.cssSelector("#app > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div > div.item-list > div.mod-fil-fans > div:nth-child(1) > span"));
        String follows = webElement_follow.getText();
        System.out.println(webElement_follow.getText());

        // 粉丝数
        WebElement webElement_fan = driver.findElement(By.cssSelector("#app > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div > div.item-list > div.mod-fil-fans > div:nth-child(2) > span"));
        String fans = webElement_fan.getText();
        System.out.println(webElement_fan.getText());

        // 写入数据库
        String uid = UID;
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql:///wbspider","wbSpider","spider");
        CallableStatement pst = connection.prepareCall("insert into user_info (uid, follows, fans) values ('" + uid + "','" + follows + "', '" + fans +"')");
        pst.execute();
        connection.close();
        pst.close();

        Thread.sleep(1000);
        driver.close();
        driver.quit();
    }

    public static void main(String[] args) {
        try {
            new SeleniumSpider("6284091238").run();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
