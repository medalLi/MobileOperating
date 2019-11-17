package com.bean;

/**
 * @author medal
 * @create 2019-11-09 11:21
 **/
public class jsonBean {

    private String bussinessRst ; // 业务结果
    private String channelCode; // 渠道编码
    private String chargefee; // 订单的充值金额
    private String clientIp; // 客户端ip
    private String gateway_id; //支付平台id,目前是手机支付
    private String idType; // 中国移动用户标识类型
    private String interFacRst; // 接口结果
    private String logOutTime; // 日志输出时间
    private String orderId; // 订单号
    private String phoneno; // 被充值的手机号
    private String provinceCode; // 省份编码
    private String receiveNotifyTime; //接到支付通知时间
    private String requestId; // 订单流水号
    private String resultTime; // 省BOSS充值处理时间
    private String retMsg; // 失败原因
    private String serverIp; // 服务器IP
    private String serverPort; // 接口服务器端口
    private String serviceName; // 接口服务名
    private String sysId;  //平台编码

    public jsonBean(String bussinessRst, String channelCode, String chargefee, String clientIp, String gateway_id, String idType, String interFacRst, String logOutTime, String orderId, String phoneno, String provinceCode, String receiveNotifyTime, String requestId, String resultTime, String retMsg, String serverIp, String serverPort, String serviceName, String sysId) {
        this.bussinessRst = bussinessRst;
        this.channelCode = channelCode;
        this.chargefee = chargefee;
        this.clientIp = clientIp;
        this.gateway_id = gateway_id;
        this.idType = idType;
        this.interFacRst = interFacRst;
        this.logOutTime = logOutTime;
        this.orderId = orderId;
        this.phoneno = phoneno;
        this.provinceCode = provinceCode;
        this.receiveNotifyTime = receiveNotifyTime;
        this.requestId = requestId;
        this.resultTime = resultTime;
        this.retMsg = retMsg;
        this.serverIp = serverIp;
        this.serverPort = serverPort;
        this.serviceName = serviceName;
        this.sysId = sysId;
    }

    public String getBussinessRst() {
        return bussinessRst;
    }

    public void setBussinessRst(String bussinessRst) {
        this.bussinessRst = bussinessRst;
    }

    public String getChannelCode() {
        return channelCode;
    }

    public void setChannelCode(String channelCode) {
        this.channelCode = channelCode;
    }

    public String getChargefee() {
        return chargefee;
    }

    public void setChargefee(String chargefee) {
        this.chargefee = chargefee;
    }

    public String getClientIp() {
        return clientIp;
    }

    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }

    public String getGateway_id() {
        return gateway_id;
    }

    public void setGateway_id(String gateway_id) {
        this.gateway_id = gateway_id;
    }

    public String getIdType() {
        return idType;
    }

    public void setIdType(String idType) {
        this.idType = idType;
    }

    public String getInterFacRst() {
        return interFacRst;
    }

    public void setInterFacRst(String interFacRst) {
        this.interFacRst = interFacRst;
    }

    public String getLogOutTime() {
        return logOutTime;
    }

    public void setLogOutTime(String logOutTime) {
        this.logOutTime = logOutTime;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getPhoneno() {
        return phoneno;
    }

    public void setPhoneno(String phoneno) {
        this.phoneno = phoneno;
    }

    public String getProvinceCode() {
        return provinceCode;
    }

    public void setProvinceCode(String provinceCode) {
        this.provinceCode = provinceCode;
    }

    public String getReceiveNotifyTime() {
        return receiveNotifyTime;
    }

    public void setReceiveNotifyTime(String receiveNotifyTime) {
        this.receiveNotifyTime = receiveNotifyTime;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getResultTime() {
        return resultTime;
    }

    public void setResultTime(String resultTime) {
        this.resultTime = resultTime;
    }

    public String getRetMsg() {
        return retMsg;
    }

    public void setRetMsg(String retMsg) {
        this.retMsg = retMsg;
    }

    public String getServerIp() {
        return serverIp;
    }

    public void setServerIp(String serverIp) {
        this.serverIp = serverIp;
    }

    public String getServerPort() {
        return serverPort;
    }

    public void setServerPort(String serverPort) {
        this.serverPort = serverPort;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getSysId() {
        return sysId;
    }

    public void setSysId(String sysId) {
        this.sysId = sysId;
    }

    @Override
    public String toString() {
        return "jsonBean{" +
                "bussinessRst='" + bussinessRst + '\'' +
                ", channelCode='" + channelCode + '\'' +
                ", chargefee='" + chargefee + '\'' +
                ", clientIp='" + clientIp + '\'' +
                ", gateway_id='" + gateway_id + '\'' +
                ", idType='" + idType + '\'' +
                ", interFacRst='" + interFacRst + '\'' +
                ", logOutTime='" + logOutTime + '\'' +
                ", orderId='" + orderId + '\'' +
                ", phoneno='" + phoneno + '\'' +
                ", provinceCode='" + provinceCode + '\'' +
                ", receiveNotifyTime='" + receiveNotifyTime + '\'' +
                ", requestId='" + requestId + '\'' +
                ", resultTime='" + resultTime + '\'' +
                ", retMsg='" + retMsg + '\'' +
                ", serverIp='" + serverIp + '\'' +
                ", serverPort='" + serverPort + '\'' +
                ", serviceName='" + serviceName + '\'' +
                ", sysId='" + sysId + '\'' +
                '}';
    }

}
