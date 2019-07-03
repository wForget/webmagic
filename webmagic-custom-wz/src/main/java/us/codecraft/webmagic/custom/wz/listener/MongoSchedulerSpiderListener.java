package us.codecraft.webmagic.custom.wz.listener;

import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.SpiderListener;
import us.codecraft.webmagic.custom.wz.scheduler.MongoScheduler;

/**
 * 爬虫监听器，成功失败后修改 MongoScheduler 中的 Request 状态。 <br>
 * 可以在 PageProcessor 中添加其他状态。 <br>
 */
public class MongoSchedulerSpiderListener implements SpiderListener {

    private MongoScheduler mongoScheduler;

    public MongoSchedulerSpiderListener (MongoScheduler mongoScheduler) {
        this.mongoScheduler = mongoScheduler;
    }

    // REQUEST_STATUS 请求状态 1:成功 2:失败  可在 PageProcessor 中添加其他状态
    public static final String REQUEST_STATUS = "_request_status";

    @Override
    public void onSuccess(Request request) {
        int requestStatus = 1;
        if (request.getExtra(REQUEST_STATUS) != null) {
            requestStatus = (Integer) request.getExtra(REQUEST_STATUS);
        }
    }

    @Override
    public void onError(Request request) {
        int requestStatus = 2;
        if (request.getExtra(REQUEST_STATUS) != null) {
            requestStatus = (Integer) request.getExtra(REQUEST_STATUS);
        }
    }

    private void changeRequestStatus(Request request, Integer status) {
        if (status != 1) {
            this.mongoScheduler.changeRequestStatus(request, status);
        }
    }

}
