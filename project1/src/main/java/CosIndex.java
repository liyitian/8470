import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import java.util.*;

/**
 * Created by tiger on 4/12/16.
 */
public class CosIndex
{

    private SparkConf config;
    private JavaSparkContext context;

    public CosIndex()
    {

        // set up Spark
        config = new SparkConf().setMaster("local[4]")
                .setAppName("Boolean Search System")
                .set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec")
                .set("spark.hadoop.validateOutputSpecs", "false")
                .set("spark.driver.maxResultSize","10G")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        context = new JavaSparkContext(config);
    }

    // Load a text file
    public JavaRDD<String> loadData(String file)
    {
        return context.textFile(file);
    }

    public static void main(String[] args) {
        // set up and load file
        BM25Calculator bmc = new BM25Calculator();
//        final Loader loader = new Loader();
        // transform the data into block entries

        //known values
        //average document length
        final double averageLength = 226.791644;
        //overall number of document
        final double N = 1613582.0;
        //other parameters
        final double k1 = 1.2;
        final double b = 0.75;

        JavaRDD<String> rdd = null;

        // default parameters
//        String inDirectory = "user/yli25/data/data";
//        String outDirectory = "user/yli25/output";

        String inDirectory = "output/finalData";
        String outDirectory = "output";

        JavaRDD<String> tmp = bmc.loadData(inDirectory + "/part-*").cache();
        JavaRDD<String> uniword = bmc.loadData(outDirectory + "/uniwordSequence/part-*").cache();
        //word,1,2,3
        JavaPairRDD<String, String> uniwordpair = uniword.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) throws Exception {
                String[] elements = s.replaceAll("(\\()|(\\]\\))","").split(",\\[");
                String word = elements[0];
                String ids = elements[1];
                return new Tuple2<String, String>(word, ids);
            }
        }).cache();
        //get document length list
        //word,1,2,3
//        List<Tuple2<String, String>> uniwordlist = uniwordpair.collect();


        JavaRDD<News> news = tmp.map(new Function<String, News>() {
            public News call(String s) throws Exception {
                String[] elements = s.split("\\$\\$");
                if(!elements[0].matches("^[0-9]*$")) {
//                    System.out.println(elements[0]);
                    News err = new News();
                    err.setText(elements[0]);
                    err.setItemid(0);
                    return err;
                }

                Integer itemid = Integer.parseInt(elements[0]);
                String title = elements[2];
                String headline = elements[3];
                String byline = elements[4];
                String text = elements[6];
                News news = new News();
                news.setItemid(itemid);
                news.setTitle(title);
                news.setHeadline(headline);
                news.setByline(byline);
                news.setText(text);
                return news;
            }
        }).filter(new Function<News, Boolean>() {
            public Boolean call(News news) throws Exception {
                if(news.getItemid()==0)
                    return false;
                else
                    return true;
            }
        });
        tmp.unpersist();

        //id, text
        JavaPairRDD<Integer, News> text = news.mapToPair(new PairFunction<News, Integer, News>() {
            public Tuple2<Integer, News> call(News news) throws Exception {
                String text = news.getTitle()+" "+news.getByline()+" "+news.getText();
                Integer id = news.getItemid();
                news.setText(text.toLowerCase());
                return new Tuple2<Integer, News>(id, news);
            }
        }).coalesce(20);

//        text.saveAsTextFile(outDirectory+"/idTextIndex");

        //id,text
        final List<Tuple2<Integer, News>> idTextList = text.collect();
        Hashtable<Integer, News> idHashtabletemp = new Hashtable<Integer, News>();

        for(Tuple2<Integer, News> idtext: idTextList){
            idHashtabletemp.put(idtext._1, idtext._2);
        }
        final Hashtable<Integer, News> idHashtable = idHashtabletemp;

        //(wainwright,WordInfo{word='wainwright', documentsRank=[(658645,0.02007608936644014)]})
        JavaPairRDD<String, WordInfo> countrdd = uniwordpair.mapToPair(new PairFunction<Tuple2<String, String>, String, WordInfo>() {
            public Tuple2<String, WordInfo> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String word = stringStringTuple2._1;
                String[] documents = stringStringTuple2._2.split(",");
                double df = documents.length - 1;
                double idf = Math.log(N/df);
                int i = 0;
                //store rank
                ArrayList<RankInfo> list = new ArrayList<RankInfo>();
                ArrayList<RankInfo> initial = new ArrayList<RankInfo>();
                RankInfo ri = new RankInfo();
                initial.add(ri);
                WordInfo wi = new WordInfo();
                wi.setWord(word);
                wi.setDocRank(initial);
                for(String id : documents){
                    //skip the first element which is frequency
                    if(i == 0) {i++; continue;}
                    int idkey = Integer.parseInt(id.trim());
                    if(idHashtable.containsKey(idkey)){
                        News news = idHashtable.get(idkey);
                        String text = news.getText();
                        //get tf df from text of specific id
                        Iterator<String> s = Arrays.asList(text.replaceAll("(\\pS)|(\\d+)","")
                                .replaceAll("(\\pP)|(\\s)|(null)|(amp)|(quot)", " ").split(" ")).iterator();
                        int itemid = Integer.parseInt(id.trim());
                        String word1 = "";
                        int length = 0;//document length
                        int wordCount = 0;//quantity of word

                        //scan text
                        while (s.hasNext()) {

                            word1 = s.next();
                            if (word1.equals("")) {
                                continue;
                            }else{
                                length++;
                                if(word1.equals(word))
                                    wordCount++;
                            }

                        }
                        double tf = Double.parseDouble(""+wordCount);//Double.parseDouble(""+length);
                        double tf_n = 1+Math.log(tf);

                        double cos = (tf_n)*idf;
                        RankInfo newri = new RankInfo();
                        newri.setId(itemid);
                        newri.setRank(cos);
                        newri.setHeadline(news.getHeadline());
                        list.add(newri);
                        wi.setDlength(length);
                        wi.setIdf(idf);
                        wi.setTf(tf);
                        wi.setWord(word);
                        wi.setDocRank(list);
                    }

                }

                return new Tuple2<String, WordInfo>(word, wi);
            }
        }).filter(new Function<Tuple2<String, WordInfo>, Boolean>() {
            public Boolean call(Tuple2<String, WordInfo> stringWordInfoTuple2) throws Exception {
                if(!(stringWordInfoTuple2._2.getDocRank().get(0).getId()==0))
                    return true;
                else
                return false;
            }
        }).reduceByKey(new Function2<WordInfo, WordInfo, WordInfo>() {
            public WordInfo call(WordInfo wordInfo, WordInfo wordInfo2) throws Exception {
                ArrayList<RankInfo> list = new ArrayList<RankInfo>();
                list.addAll(wordInfo.getDocRank());
                list.addAll(wordInfo2.getDocRank());
                wordInfo.setDocRank(list);
                return wordInfo;
            }
        }).coalesce(1);
        uniwordpair.unpersist();
        countrdd.saveAsTextFile(outDirectory+"/cos");

        JavaPairRDD<String, WordInfo> output = countrdd.mapToPair(new PairFunction<Tuple2<String, WordInfo>, String, WordInfo>() {
            public Tuple2<String, WordInfo> call(Tuple2<String, WordInfo> stringWordInfoTuple2) throws Exception {

                ArrayList<RankInfo>list = stringWordInfoTuple2._2.getDocRank();

                Collections.sort(list, new Comparator<RankInfo>() {
                    public int compare(RankInfo o1, RankInfo o2) {
                        Double r1 = o1.getRank();
                        Double r2 = o2.getRank();
                        return -(r1.compareTo(r2));
                    }
                });
                stringWordInfoTuple2._2.setDocRank(list);
                return stringWordInfoTuple2;
            }
        });

        output.saveAsTextFile(outDirectory+"/sortedCos");
        System.out.println("Success!");

    }
}
