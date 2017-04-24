import java.io.Serializable;

/**
 * Created by osboxes on 18/04/17.
 */
public class RankInfo implements Serializable {
    private int id;
    private double rank;
    private String headline;

    RankInfo(){
        id = 0;
        rank = 0.0;
        headline = "";
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public double getRank() {
        return rank;
    }

    public void setRank(double rank) {
        this.rank = rank;
    }

    public String getHeadline() {
        return headline;
    }

    public void setHeadline(String headline) {
        this.headline = headline;
    }

    @Override
    public String toString() {
        return "RankInfo{" +
                "id=" + id +
                ",rank=" + rank +
                ",headline='" + headline + '\'' +
                '}';
    }
}
