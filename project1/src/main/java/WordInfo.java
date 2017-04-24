import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by root on 25/02/17.
 */
public class WordInfo implements Serializable {

    private String word;
    private Double tf;
    private Double idf;
    private Double tfidf;
    private Integer dlength;//present document length
    private ArrayList<Integer> value;
    private ArrayList<Tuple2<Integer, Double>> documentsRank;
    private ArrayList<RankInfo> docRank;

    WordInfo(){}

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Double getTf() {
        return tf;
    }

    public void setTf(Double tf) {
        this.tf = tf;
    }

    public Double getIdf() {
        return idf;
    }

    public void setIdf(Double idf) {
        this.idf = idf;
    }

    public Double getTfidf() {
        return tfidf;
    }

    public void setTfidf(Double tfidf) {
        this.tfidf = tfidf;
    }

    public Integer getDlength() {
        return dlength;
    }

    public void setDlength(Integer dlength) {
        this.dlength = dlength;
    }

    public ArrayList<Integer> getValue() {
        return value;
    }

    public void setValue(ArrayList<Integer> value) {
        this.value = value;
    }

    public ArrayList<Tuple2<Integer, Double>> getDocumentsRank() {
        return documentsRank;
    }

    public void setDocumentsRank(ArrayList<Tuple2<Integer, Double>> documentsRank) {
        this.documentsRank = documentsRank;
    }

    public ArrayList<RankInfo> getDocRank() {
        return docRank;
    }

    public void setDocRank(ArrayList<RankInfo> docRank) {
        this.docRank = docRank;
    }

    @Override
    public String toString() {
        return "WordInfo{" +
                "word='" + word + '\'' +
                ", idf=" + idf +
                ", docRank=" + docRank +
                '}';
    }
}
