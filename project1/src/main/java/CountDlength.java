import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.util.AccumulatorV2;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import scala.Tuple2;
import scala.tools.cmd.Spec;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by tiger on 4/12/16.
 */
public class CountDlength
{

    private SparkConf config;
    private JavaSparkContext context;

    public CountDlength()
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

    public static void main(String[] args) {
        // set up and load file
        CountDlength bi = new CountDlength();
//        final Loader loader = new Loader();
        // transform the data into block entries

        JavaRDD<String> rdd = null;

        // default parameters
        String inDirectory = "finalData";
        String outDirectory = "output";

                JavaRDD<String> tmp = bi.loadData(inDirectory + "/part-*");

                if (rdd == null) rdd = tmp;
                else rdd = rdd.union(tmp);

        JavaRDD<News> news = rdd.map(new Function<String, News>() {
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


        //extract text and ids
        JavaPairRDD<String, Integer> text = news.mapToPair(new PairFunction<News, String, Integer>() {
            public Tuple2<String, Integer> call(News news) throws Exception {
                String text = news.getTitle()+" "+news.getByline()+" "+news.getText();
                Integer id = news.getItemid();
                return new Tuple2<String, Integer>(text.toLowerCase(), id);
            }
        });

        JavaPairRDD<Integer, Integer> countrdd = text.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Integer>, Integer, Integer>() {
            public Iterator<Tuple2<Integer, Integer>> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                Iterator<String> s = Arrays.asList(stringIntegerTuple2._1
                        .replaceAll("(\\pP)|(\\pS)|(\\s)|(\\d+)|(null)|(amp)|(quot)", " ").split(" ")).iterator();
                int itemid = stringIntegerTuple2._2;
                String word = "";
                int length = 0;
                List<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();

                while (s.hasNext()) {

                    word = s.next();
                    if (word.equals("")) {
                        continue;
                    }else{
                        length++;
                    }

                }
                list.add(new Tuple2<Integer, Integer>(itemid, length));

                return list.iterator();
            }
        });

        JavaPairRDD<Integer, String> newrdd = countrdd.mapToPair(new PairFunction<Tuple2<Integer, Integer>,
                Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                String array = "";
                array = integerIntegerTuple2._2 + "," + "1";
                return new Tuple2<Integer, String>(1, array);
            }
        }).reduceByKey(new Function2<String, String, String>() {
            public String call(String s, String s2) throws Exception {
                String[] s11 = s.split(",");
                String[] s22 = s2.split(",");
                int length1 = Integer.parseInt(s11[0]);
                int length2 = Integer.parseInt(s22[0]);
                int i1 = Integer.parseInt(s11[1]);
                int i2 = Integer.parseInt(s22[1]);
                String ret = "";
                length1 += length2;
                i1 += i2;
                ret = length1+","+i1;
                return ret;
            }
        });

        newrdd.saveAsTextFile(outDirectory+"/AvgLength");

        System.out.println("Success!");

    }
}