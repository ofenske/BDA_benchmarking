public class WebLog {
    private Integer accountId;
    private Integer ipAdress;
    private Integer timestamp;

    public WebLog(Integer accountId, Integer ipAdress, Integer timestamp) {
        setAccountId(accountId);
        setIpAdress(ipAdress);
        setTimestamp(timestamp);
    }

    public Integer getAccountId() {
        return accountId;
    }

    public void setAccountId(Integer accountId) {
        this.accountId = accountId;
    }

    public Integer getIpAdress() {
        return ipAdress;
    }

    public void setIpAdress(Integer ipAdress) {
        this.ipAdress = ipAdress;
    }

    public Integer getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Integer timestamp) {
        this.timestamp = timestamp;
    }
}
