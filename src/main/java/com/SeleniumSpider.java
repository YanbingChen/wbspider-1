package com;

import org.apache.commons.lang3.tuple.Pair;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
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

    public List<String> run() {
        List<String> result = new ArrayList<String>();
        String follows = "-1";
        String fans = "-1";

        System.setProperty("webdriver.chrome.driver", "/usr/local/share/chromedriver");
        WebDriver driver = new ChromeDriver();
        driver.get(FINAL_TARGET_URL);

        try {
            Thread.sleep(2000);

            // WebElement webElement = driver.findElement(By.cssSelector(".item-list"));
            // 关注数
            WebElement webElement_follow = driver.findElement(By.cssSelector("#app > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div > div.item-list > div.mod-fil-fans > div:nth-child(1) > span"));
            follows = webElement_follow.getText();
            System.out.println(webElement_follow.getText());

            // 粉丝数
            WebElement webElement_fan = driver.findElement(By.cssSelector("#app > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div > div.item-list > div.mod-fil-fans > div:nth-child(2) > span"));
            fans = webElement_fan.getText();
            System.out.println(webElement_fan.getText());

            // 写入数据库
//        String uid = UID;
//        Class.forName("com.mysql.cj.jdbc.Driver");
//        Connection connection = DriverManager.getConnection("jdbc:mysql:///wbspider","wbSpider","spider");
//        CallableStatement pst = connection.prepareCall("insert into user_info (uid, follows, fans) values ('" + uid + "','" + follows + "', '" + fans +"')");
//        pst.execute();
//        connection.close();
//        pst.close();

            Thread.sleep(1000);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            driver.close();
            driver.quit();
        }

        result.add(UID);
        result.add(follows);
        result.add(fans);

        return result;
    }

    public static void main(String[] args) {
        new SeleniumSpider("2010021631").run();
    }
}
