package com.testDemo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bean.jsonBean;

/**
 * @author medal
 * @create 2019-11-09 12:10
 **/
public class ParseJson {
    public static void main(String[] args) {
        String line = "{\"bussinessRst\":\"0000\",\"channelCode\":\"0702\",\"chargefee\":\"3000\",\"clientIp\":\"222.128.189.192\",\"gateway_id\":\"CMPAY\",\"idType\":\"01\",\"interFacRst\":\"0000\",\"logOutTime\":\"20170412045506188\",\"orderId\":\"384670483175041374\",\"phoneno\":\"15230923265\",\"provinceCode\":\"311\",\"receiveNotifyTime\":\"20170412045506157\",\"requestId\":\"20170412045443382431638911197502\",\"resultTime\":\"20170412045459\",\"retMsg\":\"接口调用成功\",\"serverIp\":\"10.255.254.10\",\"serverPort\":\"8714\",\"serviceName\":\"reChargeNotifyReq\",\"sysId\":\"01\"}\n";
        jsonBean bean = JSON.parseObject(line, jsonBean.class);
        System.out.println(bean);

    }

}
