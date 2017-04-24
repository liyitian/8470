import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.In;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
public class RankSearch
{

    private SparkConf config;
    private JavaSparkContext context;

    public RankSearch()
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
        RankSearch rs = new RankSearch();
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

        String inDirectory = "output/filteredBM25";
        String outDirectory = "output/bm25result";

                rdd = rs.loadData(inDirectory + "/part-*");

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

        JavaPairRDD<Integer, RankInfo> idRank = wordIdRank.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, Integer, RankInfo>() {
            public Iterator<Tuple2<Integer, RankInfo>> call(Tuple2<String, String> stringStringTuple2) throws Exception {

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
                WordInfo wi = new WordInfo();
                ArrayList<Tuple2<Integer, RankInfo>> list = new ArrayList<Tuple2<Integer, RankInfo>>();
                int i=0;
                for(String dr:docrank){
                    if(i==0){i++;continue;}
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

                    ri.setId(id);
                    ri.setRank(rank);
                    ri.setHeadline(headline.replaceAll(",",""));
                    list.add(new Tuple2<Integer, RankInfo>(id, ri));
                }

                return list.iterator();
            }
        }).reduceByKey(new Function2<RankInfo, RankInfo, RankInfo>() {
            public RankInfo call(RankInfo rankInfo, RankInfo rankInfo2) throws Exception {
                double totalrank = rankInfo.getRank()+rankInfo2.getRank();
                rankInfo.setRank(totalrank);
                return rankInfo;
            }
        });

        final JavaPairRDD<Double, String> rankId = idRank.mapToPair(new PairFunction<Tuple2<Integer, RankInfo>, Double, String>() {
            public Tuple2<Double, String> call(Tuple2<Integer, RankInfo> integerDoubleTuple2) throws Exception {

                String s = integerDoubleTuple2._2.getId()+":"+integerDoubleTuple2._2.getHeadline();
                return new Tuple2<Double, String>(integerDoubleTuple2._2.getRank(), s);
            }
        }).sortByKey(false);

        List<Tuple2<Double, String>> topRecords = rankId.take(20);



        for(Tuple2<Double,String> t : topRecords)
        System.out.println("top records:" + t);

        JavaRDD<Tuple2<Double, String>> outputRdd = rs.context.parallelize(topRecords).coalesce(1);
        outputRdd.saveAsTextFile(outDirectory+"/rankSearchResult");

        System.out.println("Success!");
    }
}