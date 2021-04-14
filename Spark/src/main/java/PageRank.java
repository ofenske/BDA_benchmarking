public class PageRank {
    private Integer accountId;
    private Double pageRank;

    public PageRank(Integer accountId, Double pageRank) {
        setAccountId(accountId);
        setPageRank(pageRank);
    }

    public Integer getAccountId() {
        return accountId;
    }

    public void setAccountId(Integer accountId) {
        this.accountId = accountId;
    }

    public Double getPageRank() {
        return pageRank;
    }

    public void setPageRank(Double pageRank) {
        this.pageRank = pageRank;
    }
}
