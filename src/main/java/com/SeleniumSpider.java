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

    public void run() throws InterruptedException, ClassNotFoundException, SQLException {
        System.setProperty("webdriver.chrome.driver", "F:/21Hadoop/wbspider-1/chromedriver.exe");
        WebDriver driver = new ChromeDriver();
        driver.get(FINAL_TARGET_URL);

        Thread.sleep(3000);

        // WebElement webElement = driver.findElement(By.cssSelector(".item-list"));
        // 关注数
        WebElement webElement_follow = driver.findElement(By.cssSelector("#app > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div > div.item-list > div.mod-fil-fans > div:nth-child(1) > span"));
        double follows = Double.parseDouble(webElement_follow.getText());
        System.out.println(webElement_follow.getText());

        // 粉丝数
        WebElement webElement_fan = driver.findElement(By.cssSelector("#app > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div > div.item-list > div.mod-fil-fans > div:nth-child(2) > span"));
        double fans = Double.parseDouble(webElement_fan.getText());
        System.out.println(webElement_fan.getText());

        // 写入数据库
        double uid = Double.parseDouble(UID);
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql:///wbspider","root","admin");
        CallableStatement pst = connection.prepareCall("insert into user_info (uid, follows, fans) values (" + uid + "," + follows + ", " + fans +")");
        pst.execute();
        connection.close();
        pst.close();

        Thread.sleep(1000);
        driver.close();
        driver.quit();
    }

    public static void main(String[] args) {
        try {
            new SeleniumSpider().run();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
