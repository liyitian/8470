import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// Import factory methods provided by DataTypes.
// Import StructType and StructField
// Import Row.
// Import RowFactory.

/**
 * Created by tiger on 4/12/16.
 */
public class CosRankSearch
{

    private SparkConf config;
    private JavaSparkContext context;

    public CosRankSearch()
    {

        // set up Spark
        config = new SparkConf().setMaster("local[4]")
                .setAppName("Boolean Search System")
                .set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec")
                .set("spark.hadoop.validateOutputSpecs", "false")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        context = new JavaSparkContext(config);
    }

    // Load a text file
    public JavaRDD<String> loadData(String file)
    {
        return context.textFile(file);
    }

    public static void main(final String[] args) {
        // set up and load file
        CosRankSearch rs = new CosRankSearch();
//        final Loader loader = new Loader();
        // transform the data into block entries

        JavaRDD<String> rdd = null;


        if(args.length<1){
            System.out.println("No Arguments!");
        }

        String[] inputs = args;
        if(args.length>=1){
            for(int i=0;i<args.length;i++){
                inputs[i]=inputs[i].toLowerCase();
            }
        }
        final String[] input=inputs;



        // default parameters
//        String inDirectory = "user/yli25/data/data";
//        String outDirectory = "user/yli25/output";

        String inDirectory = "output/filteredCos";
        String outDirectory = "output/cosresult";

//                rdd = rs.loadData(inDirectory + "/part-*");
        rdd = rs.loadData(inDirectory + "/*.gz");
        
        //(abstract,216081,0.004684143756800567), (389393,0.0211429345451755), (675557,0.00989804759628778), (482790,0.09762386879693727), (404821,0.046740984770699344), (516595,0.0130384069345866), (95729,0.15989567130099958), (505123,0.028377073330880603), (433702,0.01332701194608077)
        JavaPairRDD<String, String> wordIdRank = rdd.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) throws Exception {
                String[] elements = s
                                    .split(",");
                String word = elements[0].replaceAll("\\(","").trim();

                return new Tuple2<String, String>(word, s);
            }
        }).filter(new Function<Tuple2<String, String>, Boolean>() {
            public Boolean call(Tuple2<String, String> stringStringTuple2) throws Exception {

                for(String word : input){
                    if(word.equals(stringStringTuple2._1))
                        return true;
                }

                return false;
            }
        });

        JavaPairRDD<Integer, ArrayList<WordInfo>> idWordList = wordIdRank.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, Integer, ArrayList<WordInfo>>() {
            public Iterator<Tuple2<Integer, ArrayList<WordInfo>>> call(Tuple2<String, String> stringStringTuple2) throws Exception {

                //alamosa,WordInfo{word='alamosa',
                //14.293967110378958, docRank=RankInfo{id=757478,rank=17.98570241495611,headline='Zions Bancorp to buy Sky Valley Bank.'}
                String[] elements = stringStringTuple2._2.replaceAll("(\\[)|(\\]}\\))|(\\()","")
                        .split("(idf=)");
                String name = stringStringTuple2._1;

                String[] ele = elements[1].split(",");
                double idf;
                if(ele[0].trim().equals("null"))
                    idf=0.0;
                else
                    idf = Double.parseDouble(ele[0].trim());


                //id=757478,rank=17.98570241495611,headline='Zions Bancorp to buy Sky Valley Bank.'}
                //id rank headline ...
                String[] docrank = elements[1].split("(RankInfo\\{)");


                ArrayList<Tuple2<Integer, ArrayList<WordInfo>>> list = new ArrayList<Tuple2<Integer,  ArrayList<WordInfo>>>();
                int i=0;
                for(String dr:docrank){
                    if(i==0){i++;continue;}
                    ArrayList<WordInfo> wordlist = new ArrayList<WordInfo>();
                    RankInfo ri = new RankInfo();
                    String[] rankelement = dr.trim().replaceAll("('})","").split("(,headline=')");
                    //id=757478,rank=17.98570241495611
                    String[] idrank = rankelement[0].split(",");
                    int id = Integer.parseInt(idrank[0].trim().replaceAll("(id=)",""));
                    double rank = Double.parseDouble(idrank[1].trim().replaceAll("(rank=)",""));
                    String headline;
                    if(rankelement.length<2)
                        headline="";
                    else
                        headline = rankelement[1];
                    WordInfo wi = new WordInfo();
                    wi.setIdf(idf);
                    wi.setWord(name);
                    ArrayList<RankInfo> ranklist = new ArrayList<RankInfo>();
                    ri.setId(id);
                    ri.setRank(rank);
                    ri.setHeadline(headline.replaceAll(",",""));
                    ranklist.add(ri);
                    wi.setDocRank(ranklist);
                    wordlist.add(wi);
                    list.add(new Tuple2<Integer,  ArrayList<WordInfo>>(id, wordlist));
                }

                return list.iterator();

            }
        }).reduceByKey(new Function2<ArrayList<WordInfo>, ArrayList<WordInfo>, ArrayList<WordInfo>>() {
            public ArrayList<WordInfo> call(ArrayList<WordInfo> wordInfos, ArrayList<WordInfo> wordInfos2) throws Exception {
                wordInfos.addAll(wordInfos2);
                return wordInfos;
            }
        });




        final JavaPairRDD<Double, String> rankId = idWordList.mapToPair(new PairFunction<Tuple2<Integer, ArrayList<WordInfo>>, Double, String>() {
            public Tuple2<Double, String> call(Tuple2<Integer, ArrayList<WordInfo>> integerArrayListTuple2) throws Exception {
                int id = integerArrayListTuple2._1;

                ArrayList<Tuple2<Double, Double>> qd = new ArrayList<Tuple2<Double, Double>>();

                String headline = "";

                for(WordInfo wi:integerArrayListTuple2._2){
                    double idf = wi.getIdf();
                    String word = wi.getWord();
                    RankInfo ri = wi.getDocRank().get(0);
                    double tfidf = ri.getRank();
                    headline = ri.getHeadline().replaceAll("(&amp;)","&");
                    int count=0;
                    for(String w : input){
                        if(w.equals(word)) {
                            count++;
                        }
                    }

                    for(String w : input){
                        if(w.equals(word)) {
                            double tf = count;
//                            double tf_n = tf/input.length;
                            double tf_n = 1 + Math.log(tf);
                            double qtfidf = (tf_n) * idf;
                            qd.add(new Tuple2<Double, Double>(qtfidf, tfidf));
                        }
                    }
                }
                double dot = 0.0;
                double q = 0.0;
                double d = 0.0;
                double finaltfidf = 0.0;
                for(Tuple2<Double,Double> tmp:qd){
                    dot+=(tmp._1*tmp._2);
                    q+= (tmp._1*tmp._1);
                    d+= (tmp._2*tmp._2);
                    finaltfidf+=tmp._2;
                }



                q = Math.sqrt(q);
                d = Math.sqrt(d);

                double cosqd = dot/(q*d);
                String s = integerArrayListTuple2._1 +","+ headline;
//                return new Tuple2<Double, String>(integerArrayListTuple2._2.get(0).getDocRank().get(0).getRank(), s);
                return new Tuple2<Double, String>(finaltfidf/2.0, s);

            }
        }).sortByKey(false)
//                .filter(new Function<Tuple2<Double, String>, Boolean>() {
//            public Boolean call(Tuple2<Double, String> doubleStringTuple2) throws Exception {
//
//                StringBuilder s = new StringBuilder();
//                String s2 = doubleStringTuple2._2.toLowerCase();
//                String s3 = s2.replaceAll("\\."," ");
//                String s4 = input[0].substring(0,1)+" "+input[1].substring(0,1)+" ";
//                for(String w : input){
//                    s.append(" "+w);
//                }
//
//                if(s2.contains(s)||s3.contains(s4)
//                        ||s2.contains(input[0])
//                        ||s2.contains(input[1])
//                        )
//                    return true;
//                else
//                    return false;
//
//            }
//        })
    ;

//        List<Tuple2<String, String>> topRecords = rankId.mapToPair(new PairFunction<Tuple2<Double,String>, String, String>() {
//            public Tuple2<String, String> call(Tuple2<Double, String> doubleStringTuple2) throws Exception {
//
//                return new Tuple2<String, String>("",doubleStringTuple2._2);
//            }
//        }).take(20);
        List<Tuple2<Double, String>> topRecords = rankId.take(20);



        for(Tuple2<Double,String> t : topRecords)
        System.out.println("top records:" + t);

        JavaRDD<Tuple2<Double, String>> outputRdd = rs.context.parallelize(topRecords).coalesce(1);
        outputRdd.saveAsTextFile(outDirectory);

        System.out.println("Success!");
    }
}