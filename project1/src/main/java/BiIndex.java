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
public class BiIndex
{

    private SparkConf config;
    private JavaSparkContext context;

    public BiIndex()
    {

        // set up Spark
        config = new SparkConf().setMaster("local[2]")
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
        BiIndex bi = new BiIndex();
        // transform the data into block entries

        JavaRDD<String> rdd = null;

        String inDirectory = "finalData";
        String outDirectory = "output";

                JavaRDD<String> tmp = bi.loadData(inDirectory + "/part-00000");

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

        JavaPairRDD<String, ArrayList<Integer>> singleWord = text.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Integer>, String, ArrayList<Integer>>() {
            public Iterator<Tuple2<String, ArrayList<Integer>>> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                List<Tuple2<String, ArrayList<Integer>>> list = new ArrayList<Tuple2<String, ArrayList<Integer>>>();
                Iterator<String> s = Arrays.asList(stringIntegerTuple2._1
                        .replaceAll(
                "(\\pP)|(\\pS)|(\\d)|(\\s)|(null)|(amp)|(quot)|(a)|(the)|(of)|(an)|(that)|(at)|(in)|(without)|(within)" +
                        "|(between)|(this)|(these)|(those)|(their)|(his)|(her)|(its)|(my)", " ").split(" ")).iterator();
                ArrayList<Integer> values = new ArrayList<Integer>();
                values.add(1);
                values.add(stringIntegerTuple2._2);
                String first = "", second = "";

                while (s.hasNext()) {
                    while (s.hasNext()) {
                        first = s.next();
                        if(!second.equals("")) {first = second; break;}
                        if (first.equals("")) continue;
                        else break;
                    }

                    while (s.hasNext()) {
                        second = s.next();
                        if (second.equals("")) continue;
                        else break;
                    }

                    list.add(new Tuple2<String, ArrayList<Integer>>(first + " " + second, values));
                }

                return list.iterator();
            }
        })
               .reduceByKey(new Function2<ArrayList<Integer>, ArrayList<Integer>, ArrayList<Integer>>() {
                   public ArrayList<Integer> call(ArrayList<Integer> integers, ArrayList<Integer> integers2) throws Exception {
                       ArrayList<Integer> ret = new ArrayList<Integer>();
                       ret.add(integers.get(0) + integers2.get(0));
                       int length1 = integers.size();
                       int length2 = integers2.size();

                       for(int i = 1; i<length1; i++)
                           ret.add(integers.get(i));


                       for(int i = 1; i<length2; i++)
                           ret.add(integers2.get(i));

                       Set<Integer> set = new HashSet<Integer>();
                       Iterator<Integer> iter = ret.iterator();
                       int count = 0;

                       while (iter.hasNext()) {
                           int id = iter.next();
                           if (count == 0) {count++; continue;}

                           if (!set.add(id))
                               iter.remove();
                       }

                       return ret;
                   }
               }).repartition(20).sortByKey();

        singleWord.saveAsTextFile(outDirectory+"/biwordSequence");

        System.out.println("Success!");
    }
}