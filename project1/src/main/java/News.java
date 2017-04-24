import java.io.Serializable;
import java.util.List;

/**
 * Created by root on 10/02/17.
 */
public class News implements Serializable {

    private int itemid;
    private String date;
    private String title;
    private String headline;
    private String byline;
    private String dateline;
    private String text;
    private String metadata;
    private List<String> countriescodes;
    private List<String> industriescodes;
    private List<String> topicscodes;


    News(){}

    public int getItemid() {
        return itemid;
    }

    public void setItemid(int itemid) {
        this.itemid = itemid;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getHeadline() {
        return headline;
    }

    public void setHeadline(String headline) {
        this.headline = headline;
    }

    public String getByline() {
        return byline;
    }

    public void setByline(String byline) {
        this.byline = byline;
    }

    public String getDateline() {
        return dateline;
    }

    public void setDateline(String dateline) {
        this.dateline = dateline;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    public List<String> getCountriescodes() {
        return countriescodes;
    }

    public void setCountriescodes(List<String> countriescodes) {
        this.countriescodes = countriescodes;
    }

    public List<String> getIndustriescodes() {
        return industriescodes;
    }

    public void setIndustriescodes(List<String> industriescodes) {
        this.industriescodes = industriescodes;
    }

    public List<String> getTopicscodes() {
        return topicscodes;
    }

    public void setTopicscodes(List<String> topicscodes) {
        this.topicscodes = topicscodes;
    }

    @Override
    public String toString() {
        return
                itemid +
                "$$" + date  +
                "$$" + title +
                "$$" + headline +
                "$$" + byline +
                "$$" + dateline +
                "$$" + text +
                "$$" + countriescodes +
                "$$" + industriescodes +
                "$$" + topicscodes
                ;
    }
}
