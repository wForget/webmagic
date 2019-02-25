package us.codecraft.webmagic.custom.wz.downloader.proxyprovider;

import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.proxy.Proxy;
import us.codecraft.webmagic.proxy.ProxyProvider;

/**
 * Created by hadoop on 2019/2/25.
 *
 * 设置本地 fiddler 代理
 */
public class LocalProxyProvider implements ProxyProvider {

    @Override
    public void returnProxy(Proxy proxy, Page page, Task task) {

    }

    @Override
    public Proxy getProxy(Task task) {
        return new Proxy("127.0.0.1", 8888);
    }
}
