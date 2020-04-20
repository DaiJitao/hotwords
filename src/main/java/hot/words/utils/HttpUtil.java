package hot.words.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.*;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.X509HostnameVerifier;
import org.apache.http.cookie.Cookie;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by daijitao on 16/5/14.
 */
public class HttpUtil {

    private final static Logger logger = LoggerFactory.getLogger(HttpUtil.class);

    private final static String HTTPS = "https";

    private final static String HTTP = "http";

    /**
     * Get 请求 不指定请求参数
     *
     * @param url
     * @return
     */
    public static String doGet(String url) {
        return doGet(url, null, null);
    }

    /**
     * Get请求 指定请求参数
     *
     * @param url
     * @param params
     * @return
     */
    public static String doGet(String url, Map<String, String> params) {
        return doGet(url, params, null);
    }

    /**
     * Post请求 不指定请求参数
     *
     * @param url
     * @return
     */
    public static String doPost(String url) {
        return doPost(url, null, null);
    }

    /**
     * Post请求 指定请求参数
     *
     * @param url
     * @param params
     * @return
     */
    public static String doPost(String url, Map<String, String> params) {
        return doPost(url, params, null);
    }

    /**
     * Get请求 指定参数、cookies
     *
     * @param url     远程地址
     * @param params  参数
     * @param cookies cookie信息
     * @return
     */
    public static String doGet(String url, Map<String, String> params, Cookie[] cookies) {
        long start = System.currentTimeMillis();

        if (StringUtils.isEmpty(url)) {
            throw new IllegalArgumentException("url为空");
        }

        CloseableHttpClient httpClient = createHttpClientByUrl(url);
        CloseableHttpResponse httpResponse = null;
        HttpGet httpGet = null;
        String result;

        try {
            url = addParams(url, params);
            httpGet = new HttpGet(url);
            addCookies(httpGet, cookies);
            httpResponse = httpClient.execute(httpGet);
            result = CharStreams.toString(new InputStreamReader(httpResponse.getEntity().getContent(), Consts.UTF_8));
        } catch (Exception e) {
            throw new IllegalStateException("Http doGost 异常", e);
        } finally {
            if (httpGet != null) {
                httpGet.releaseConnection();
            }
            IOUtils.closeQuietly(httpResponse);
            IOUtils.closeQuietly(httpClient);
        }

        long time = System.currentTimeMillis() - start;
        logger.info("post请求url:{} , 花费时间:{} ms", url, time);
        return result;
    }

    /**
     * Post请求 指定参数、cookies
     *
     * @param url     远程连接地址
     * @param params  参数
     * @param cookies cookie信息
     * @return
     */
    public static String doPost(String url, Map<String, String> params, Cookie[] cookies) {
        long start = System.currentTimeMillis();

        if (Strings.isNullOrEmpty(url)) {
            throw new IllegalArgumentException("url为空");
        }

        CloseableHttpClient httpClient = createHttpClientByUrl(url);
        HttpPost httpPost = null;
        String result;

        try {
            httpPost = new HttpPost(url);
            addParams(httpPost, params);
            addCookies(httpPost, cookies);
            ResponseHandler<String> responseHandler = new BasicResponseHandler();
            result = httpClient.execute(httpPost, responseHandler);
        } catch (Exception e) {
            throw new IllegalStateException("Http doPost 异常", e);
        } finally {
            if (httpPost != null) {
                httpPost.releaseConnection();
            }
            IOUtils.closeQuietly(httpClient);
        }

        long time = System.currentTimeMillis() - start;
        logger.info("post请求url:{} , 花费时间:{} ms", url, time);
        return result;
    }

    private static String addParams(String url, Map<String, String> params) {
        if (params != null && params.size() > 0) {
            StringBuilder builder = new StringBuilder();
            builder.append(url);

            if (url.contains("?")) {
                builder.append("&");
            } else {
                builder.append("?");
            }

            for (String key : params.keySet()) {
                if (params.get(key) == null || "".equals(params.get(key).trim())) {
                    continue;
                }
                String value = params.get(key).trim().replaceAll(" ", "%20").replaceAll("\t", "");
                builder.append(key).append("=").append(value).append("&");
            }

            url = builder.toString();

            if (url.endsWith("&")) {
                url = url.substring(0, url.lastIndexOf("&"));
            }
        }

        return url;
    }

    private static void addParams(HttpPost httpPost, Map<String, String> params) {
        if (params != null && params.size() > 0) {
            List<NameValuePair> paramList = new ArrayList<>();

            for (Map.Entry<String, String> entry : params.entrySet()) {
                String key = entry.getKey();

                if (params.get(key) == null || "".equals(params.get(key).trim())) {
                    continue;
                }

                String value = params.get(key).trim().replaceAll(" ", "%20").replaceAll("\t", "");
                paramList.add(new BasicNameValuePair(entry.getKey(), value));
            }

            UrlEncodedFormEntity entity = new UrlEncodedFormEntity(paramList, Consts.UTF_8);
            httpPost.setEntity(entity);
        }
    }

    private static void addCookies(HttpGet httpGet, Cookie[] cookies) {
        String cookieStr = assembleCookies(cookies);

        if (StringUtils.isNotEmpty(cookieStr)) {
            httpGet.setHeader("Cookie", cookieStr);
        }
    }

    private static void addCookies(HttpPost httpPost, Cookie[] cookies) {
        String cookieStr = assembleCookies(cookies);

        if (StringUtils.isNotEmpty(cookieStr)) {
            httpPost.setHeader("Cookie", cookieStr);
        }
    }

    private static String assembleCookies(Cookie[] cookies) {
        if (cookies == null || cookies.length == 0) {
            return StringUtils.EMPTY;
        }

        StringBuilder builder = new StringBuilder();
        for (Cookie cookie : cookies) {
            builder.append(cookie.getName());
            builder.append("=");
            builder.append(cookie.getValue());
            builder.append(";");
        }

        return builder.toString();
    }

    public static String doRequestForGift(HttpUriRequest request, String encoding) {
        // TODO 后期需要兼容HTTPS
        CloseableHttpClient httpClient = HttpClientBuilder.create().build();
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(request);
            StatusLine statusLine = response.getStatusLine();
            if (statusLine.getStatusCode() == HttpStatus.SC_OK) {
                HttpEntity entity = response.getEntity();
                ContentType contentType = ContentType.getOrDefault(entity);
                Charset charset = contentType.getCharset();
                String content = EntityUtils.toString(entity);
                logger.info(String.format("Success | Request gift server upload | request = %s |response : %s", request, content));
                return convertStringContent(content, charset.name(), encoding);
            } else {
                logger.info(String.format("Fail | Request gift server upload | request = %s |response : %s", request,
                        statusLine.getReasonPhrase()));
                return StringUtils.EMPTY;
            }
        } catch (IOException e) {
            logger.error(String.format("Error | Request gift server upload | request = %s ", request));
        } finally {
            close(response, httpClient);
        }
        return StringUtils.EMPTY;
    }

    private static void close(Closeable... c) {
        try {
            for (Closeable temp : c) {
                if (temp != null) {
                    temp.close();
                }
            }
        } catch (IOException e) {
            logger.error("close exception", e);
        }
    }

    private static String convertStringContent(String source, String srcEncode, String destEncode) {
        if (StringUtils.isBlank(source)) {
            return StringUtils.EMPTY;
        }
        try {
            return new String(source.getBytes(srcEncode), destEncode);
        } catch (UnsupportedEncodingException e) {
            logger.error("convertStringContent exception", e);
            throw new RuntimeException(e.getCause());
        }
    }

    private static String checkUrl(HttpUriRequest request) {
        String url = null;
        try {
            url = request.getURI().toURL().toString();
        } catch (MalformedURLException e) {
            throw new RuntimeException("get request from HttpUriRequest error");
        }

        if (StringUtils.isBlank(url)) {
            throw new RuntimeException("request can not be null or empty");
        }
        return url;
    }

    private static CloseableHttpClient createHttpClientByUrl(String url) {
        CloseableHttpClient client = null;
        if (StringUtils.equalsIgnoreCase(url.substring(0, 5), HTTPS)) {
            // TODO create https method
            try {
                KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
                HttpClientBuilder httpClientBuilder = HttpClients.custom();
                SSLContext sslcontext = SSLContexts.custom()
                        .loadTrustMaterial(trustStore, new TrustStrategy() {
                            @Override
                            public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                                return true;
                            }
                        }).build();
                SSLConnectionSocketFactory sslConnectionSocketFactory = new SSLConnectionSocketFactory(sslcontext, new X509HostnameVerifier() {
                    @Override
                    public void verify(String host, SSLSocket ssl) throws IOException {

                    }

                    @Override
                    public void verify(String host, X509Certificate cert) throws SSLException {

                    }

                    @Override
                    public void verify(String host, String[] cns, String[] subjectAlts) throws SSLException {

                    }

                    @Override
                    public boolean verify(String s, SSLSession sslSession) {
                        return true;
                    }
                });
                httpClientBuilder.setSSLSocketFactory(sslConnectionSocketFactory);
                httpClientBuilder.setSSLContext(sslcontext);
                client = httpClientBuilder.build();
            } catch (Exception e) {
                logger.warn("create httpclient fail :{}", e.getMessage(), e);
                throw new IllegalStateException("create httpclient fail", e);
            }
        } else {
            client = HttpClients.createDefault();
        }
        return client;
    }

    public static void main(String[] args) throws Exception {
        String testURL = "http://10.121.17.201:8093/hotwordNer/test";// "http://localhost:8093/hotwordNer/test";
        String hotWordsURL = "http://10.121.17.201:8093/hotwordNer/hotWords"; // "http://localhost:8093/hotwordNer/hotWords";
        //File file = new File("C:\\Users\\dell\\Desktop\\praise\\data.txt");
        //String data = FileUtil.loadData(file);
        Map<String, String> params = new HashMap<>();
        String data = "{\"taskType\":\"2\",\"content\":[{\"doc\":\"正文1\"},{\"doc\":\"正文2\"},{\"doc\":\"正文3\"}],\"taskId\":\"hotwordner_test1000_0001b6facf9d48ceb0f0d97b94d00a04\",\"topN\":\"100\"}";
        JSONObject jsonObject = JSONObject.parseObject(data);
//        System.out.println(jsonObject);
//        JSONArray jsonArray = jsonObject.getJSONArray("content");
//        System.out.println(jsonArray);
//        System.out.println(jsonArray.subList(0,2));
//        jsonObject.put("content", jsonArray.subList(0,2));
//        System.out.println(jsonObject.getJSONArray("content"));

        Long rs = 0L;
        for (int i = 0; i < 10; i++) {
            jsonObject.put("taskId", "hotwordner_test_" + UUIDGenerator.getUUID());
            params.put("taskData", jsonObject.toJSONString());

            Long start = System.currentTimeMillis();
            String doPost = doPost(hotWordsURL, params);
            Long end = System.currentTimeMillis();
            rs += (end - start);
            System.out.println(i  + " " +doPost);
        }
        System.out.println(rs);
        System.out.println(rs/100);

        String doGet = doGet(testURL);
        System.out.println(doGet);
    }
}
