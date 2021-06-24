package com;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

public class PostsSpider {
    // 话题热门广场的响应json
    public String jsonUrl;
    // 爬多少页
    public int pages = 1;

    public PostsSpider(String jsonUrl, int pages) {
        this.jsonUrl = jsonUrl;
        this.pages = pages;
    }

    public List<Pair<String, String>> run() throws InterruptedException, ClassNotFoundException, SQLException {
        // 0 Initialize Params
        List<Pair<String, String>> result = new ArrayList<>();

        CloseableHttpClient httpClient = null;
        CloseableHttpResponse response = null;

        for (int i = 0; i < pages; i++) { // 循环爬取前50页
            try {
                // 1. 生成httpclient，相当于打开一个浏览器
                httpClient = HttpClients.createDefault();

                // 2. 创建get请求
                HttpGet request = new HttpGet(jsonUrl + "&page=" + i);
                // 3. 执行get请求
                response = httpClient.execute(request);
                // 4. 判断响应状态是否为200，进行处理
                if(response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    // 5. 获取响应内容
                    HttpEntity httpEntity = response.getEntity();
                    String html = EntityUtils.toString(httpEntity, "utf-8");
                    JSONObject obj = JSONObject.parseObject(html);
                    JSONArray posts = (JSONArray)((JSONObject)obj.get("data")).get("cards"); // 获取当前页所有帖子的JSONArray
                    // System.out.println(posts);
                    // System.out.println(posts.size());
                    for (int j = 0; j < posts.size(); j++) {
                        JSONObject mblog = (JSONObject)((JSONObject)(posts.get(j))).get("mblog");
                        if (mblog == null || mblog.get("user") == null || "null".equals(mblog.get("user"))) {
                            continue;
                        }
                        // 获取发帖人的uid
                        String uid = ((JSONObject)mblog.get("user")).get("id").toString();
                        // 获取帖子文本内容
                        String text = "";
                        if (mblog.get("isLongText").toString().equals("false") || !mblog.containsKey("longText")) {
                            text = mblog.get("text").toString();
                        } else {
                            text = ((JSONObject)mblog.get("longText")).get("longTextContent").toString();
                        }
                        System.out.println(uid + ", " + text);
                        result.add(new ImmutablePair<>(uid, text));
                    }
                } else {
                    // 返回状态不是200，做错误处理
                    System.out.println("返回状态不是200");
                    System.out.println(EntityUtils.toString(response.getEntity(), "utf-8"));
                }
                // 6. 关闭
                HttpClientUtils.closeQuietly(response);
                HttpClientUtils.closeQuietly(httpClient);
            } catch (ClientProtocolException e) {
                e.printStackTrace();

            } catch (IOException e) {
                e.printStackTrace();
            }
            Thread.sleep(2000);
        }


        // Return result
        return result;
    }

    public static void main(String[] args) {
        String jsonUrl = "https://m.weibo.cn/api/container/getIndex?containerid=100103type%3D60%26q%3D%23%E8%BF%99%E6%98%AF%E8%9C%9C%E9%9B%AA%E5%86%B0%E5%9F%8E%E6%96%B0%E6%AD%8C%E5%90%97%23%26t%3D10&isnewpage=1&extparam=seat%3D1%26_position%3D800866529%26source%3Dranklist%26dgr%3D0%26filter_type%3Drealtimehot%26pos%3D0%26pre_seqid%3D740462592%26search_flag%3D0%26c_type%3D30%26mi_cid%3D100103%26flag%3D16%26cate%3D0%26display_time%3D1624292454%26pre_seqid%3D740462592&luicode=10000011&lfid=231583&page_type=searchall";
        int pages = 20;
        try {
            PostsSpider ps = new PostsSpider(jsonUrl, pages);
            ps.run();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
