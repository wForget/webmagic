package us.codecraft.webmagic.custom.wz.downloader;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Encoder;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.downloader.AbstractDownloader;
import us.codecraft.webmagic.model.HttpRequestBody;
import us.codecraft.webmagic.proxy.Proxy;
import us.codecraft.webmagic.proxy.ProxyProvider;
import us.codecraft.webmagic.selector.PlainText;
import us.codecraft.webmagic.utils.CharsetUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.*;

/**
 * Created by hadoop on 2019/2/25.
 *
 * 使用 Jsoup 下载
 */
public class JsoupDownloader extends AbstractDownloader {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private ProxyProvider proxyProvider;

    public ProxyProvider getProxyProvider() {
        return proxyProvider;
    }

    public void setProxyProvider(ProxyProvider proxyProvider) {
        this.proxyProvider = proxyProvider;
    }

    @Override
    public void setThread(int threadNum) {
    }

    @Override
    public Page download(Request request, Task task) {
        Proxy proxy = null;
        Page page = Page.fail();
        try {
            Connection connection = Jsoup.connect(request.getUrl());
            connection.header("Accept", "*/*")
                    .followRedirects(true)	// 是否跟随跳转, 处理3开头的状态码
                    .ignoreHttpErrors(true)	// 是否忽略网络错误, 处理5开头的状态码
                    .ignoreContentType(true);	// 是否忽略类型, 处理图片、音频、视频等下载

            if (request.getMethod() != null) {
                connection.method(Connection.Method.valueOf(request.getMethod()));
            }
            Site site = task.getSite();
            if ( site != null ) {
                if (site.getTimeOut() > 0) {
                    connection.timeout(site.getTimeOut());
                }
                if (site.getUserAgent() != null) {
                    connection.userAgent(site.getUserAgent());
                }
                if (site.getHeaders() != null && !site.getHeaders().isEmpty()) {
                    connection.headers(site.getHeaders());
                }
                if (site.getCookies() != null && !site.getCookies().isEmpty()) {
                    connection.cookies(site.getCookies());
                }
            }
            if (request.getHeaders() != null && !request.getHeaders().isEmpty()) {
                connection.headers(request.getHeaders());
            }
            HttpRequestBody requestBody = request.getRequestBody();
            if (requestBody != null) {
                connection.header("Content-Type", requestBody.getContentType());
                connection.postDataCharset(requestBody.getEncoding());

                byte[] body = requestBody.getBody();
                String bodyStr = null;
                try {
                    bodyStr = new String(body, requestBody.getEncoding());
                } catch (UnsupportedEncodingException e){
                    throw new UnsupportedEncodingException("requestBody encoding error, throw UnsupportedEncodingException!");
                }
                if (requestBody.getContentType() == HttpRequestBody.ContentType.FORM) {
                    List<NameValuePair> pairList = URLEncodedUtils.parse(bodyStr, Charset.forName(requestBody.getEncoding()));
                    if (pairList != null && !pairList.isEmpty()) {
                        for (NameValuePair pair: pairList) {
                            connection.data(pair.getName(), pair.getValue());
                        }
                    }
                } else {
                    connection.requestBody(bodyStr);
                }
            }
            if (proxyProvider != null) {
                proxy = proxyProvider.getProxy(task);
//                connection.proxy(new java.net.Proxy(java.net.Proxy.Type.HTTP, new InetSocketAddress(proxy.getHost(), proxy.getPort())));
                connection.proxy(proxy.getHost(), proxy.getPort());

                if (StringUtils.isNotBlank(proxy.getUsername()) && StringUtils.isNotBlank(proxy.getPassword())) {
                    //设置你的用户名和密码 例如 username=admin,password=123456
                    //String authentication = "admin:123456";
                    //需要用BASE64Encoder进行编码转换
                    //String encodedLogin = new BASE64Encoder().encode(authentication.getBytes());
                    //conn.setRequestProperty("Proxy-Authorization", " Basic " + encodedLogin);

                    String authentication = proxy.getUsername() + ":" + proxy.getPassword();
                    String encodedLogin = new BASE64Encoder().encode(authentication.getBytes());
                    connection.header("Proxy-Authorization", " Basic " + encodedLogin);
                }
            }

            Connection.Response response = connection.execute();
            page = handleResponse(request, response, task);
            onSuccess(request);
            logger.info("downloading page success {}", request.getUrl());
            return page;
        } catch (IOException e) {
            logger.warn("download page {} error", request.getUrl(), e);
            onError(request);
            return page;
        } finally {
            if (proxyProvider != null && proxy != null) {
                proxyProvider.returnProxy(proxy, page, task);
            }
        }
    }

    protected Page handleResponse(Request request, Connection.Response response, Task task) throws IOException {
        byte[] bytes = response.bodyAsBytes();
        String contentType = response.contentType() == null ? "" : response.contentType();
        Page page = new Page();
        page.setBytes(bytes);
        if (!request.isBinaryContent()){
            String charset = response.charset();
            if (charset == null) {
                charset = getHtmlCharset(contentType, bytes);
            }
            page.setCharset(charset);
            page.setRawText(new String(bytes, charset));
        }
        page.setUrl(new PlainText(request.getUrl()));
        page.setRequest(request);
        page.setStatusCode(response.statusCode());
        page.setDownloadSuccess(true);
        if (response.headers() != null && !response.headers().isEmpty()) {
            Map<String, List<String>> headers = new HashMap<String, List<String>>();
            for (Map.Entry<String, String> header: response.headers().entrySet()) {
                if (headers.containsKey(header.getKey())) {
                    headers.get(header.getKey()).add(header.getValue());
                } else {
                    List<String> values = new ArrayList<String>();
                    values.add(header.getValue());
                    headers.put(header.getKey(), values);
                }
            }
            page.setHeaders(headers);
        }

        return page;
    }

    private String getHtmlCharset(String contentType, byte[] contentBytes) throws IOException {
        String charset = CharsetUtils.detectCharset(contentType, contentBytes);
        if (charset == null) {
            charset = Charset.defaultCharset().name();
            logger.warn("Charset autodetect failed, use {} as charset. Please specify charset in Site.setCharset()", Charset.defaultCharset());
        }
        return charset;
    }
}
