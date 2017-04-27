import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.*;

// Import factory methods provided by DataTypes.
// Import StructType and StructField
// Import Row.
// Import RowFactory.

/**
 * Created by tiger on 4/12/16.
 */
public class RankFilter
{

    private SparkConf config;
    private JavaSparkContext context;
    private SQLContext sqlContext;

    public RankFilter()
    {

        // set up Spark
        config = new SparkConf().setMaster("local[4]")
                .setAppName("Boolean Search System")
                .set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec")
                .set("spark.hadoop.validateOutputSpecs", "false")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        context = new JavaSparkContext(config);
        sqlContext = new SQLContext(context);
    }

    // Load a text file
    public JavaRDD<String> loadData(String file)
    {
        return context.textFile(file);
    }

    public static void main(final String[] args) {
        // set up and load file
        RankFilter us = new RankFilter();
        // transform the data into block entries

        JavaRDD<String> rdd = null;
        String requiredWord = "";
        String[] inputWords = {""};

        if(args.length > 0){
            //todo get multiple inputs
            inputWords = args;
        }

        // default parameters
        String inDirectory = "output";
        String outDirectory = "output";

                rdd = us.loadData(inDirectory + "/sortedRank*/part-*");

        //(agerndal,WordInfo{word='agerndal', idf=null, docRank=[RankInfo{id=0,rank=0.0,headline=''}]})
        //(amalgamated,WordInfo{word='amalgamated', idf=8.391333776977591, docRank=[RankInfo{id=274280,rank=16.2321782572662,headline='Amalgamated takes 26 pct of Hyundai Oil.'}, RankInfo{id=623538,rank=15.817148480483835,headline='Asia Amalgamated 1996 net at 8.36 mln pesos.'}, RankInfo{
        JavaPairRDD<String, WordInfo> wordIdRank = rdd.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                String[] elements = s
                        .split("(idf=)");
                if(elements.length<2)
                    return false;
                else
                return true;
            }
        })
                .mapToPair(new PairFunction<String, String, WordInfo>() {
            public Tuple2<String, WordInfo> call(String s) throws Exception {
                //alamosa,WordInfo{word='alamosa',
                //14.293967110378958, docRank=RankInfo{id=757478,rank=17.98570241495611,headline='Zions Bancorp to buy Sky Valley Bank.'}
                String[] elements = s.replaceAll("(\\[)|(\\]}\\))|(\\()","")
                                    .split("(idf=)");
                String name = elements[0].split(",")[0].replaceAll("\\(","");

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
                ArrayList<RankInfo> list = new ArrayList<RankInfo>();
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
                    ri.setHeadline(headline);
                    list.add(ri);
                }

                wi.setIdf(idf);
                wi.setWord(name);
                wi.setDocRank(list);

                return new Tuple2<String, WordInfo>(name, wi);
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

                               ArrayList<RankInfo> ranklist = new ArrayList<RankInfo>();
                               int l1 = wordInfo.getDocRank().size();
                               int l2 = wordInfo2.getDocRank().size();

                               for(int i = 0; i<l1; i++)
                                   ranklist.add(wordInfo.getDocRank().get(i));

                               for(int i = 0; i<l2; i++)
                                   ranklist.add(wordInfo2.getDocRank().get(i));

                               Set<Integer> set = new HashSet<Integer>();
                               Iterator<RankInfo> iter = ranklist.iterator();
                               int count = 0;
                               iter.next();

                               while (iter.hasNext()) {
                                   RankInfo ri = iter.next();
                                   if (count == 0) {count++; set.add(ri.getId()); continue;}

                                   if (!set.add(ri.getId()))
                                       iter.remove();
                               }

                               wordInfo.setDocRank(ranklist);
                               return wordInfo;
                           }
                       }
        ).coalesce(20);

        wordIdRank.saveAsTextFile(outDirectory+"/filteredtest");

        System.out.println("Success!");
    }
}