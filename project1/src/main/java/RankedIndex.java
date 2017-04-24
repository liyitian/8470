import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by tiger on 4/12/16.
 */
public class RankedIndex
{

    private SparkConf config;
    private JavaSparkContext context;

    public RankedIndex()
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
        RankedIndex bi = new RankedIndex();
//        final Loader loader = new Loader();
        // transform the data into block entries

        JavaRDD<String> rdd = null;

        // default parameters
//        String inDirectory = "user/yli25/data/data";
//        String outDirectory = "user/yli25/output";

        String inDirectory = "output/finalData";
        String outDirectory = "output";

                JavaRDD<String> tmp = bi.loadData(inDirectory + "/part-00000");

//                ttmp = ttmp.repartition(20);

//                ttmp.saveAsTextFile(outDirectory+"/tmp");
//                JavaRDD<String> tmp = bi.loadData(outDirectory+"/tmp/part-00000");

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

//                if(elements.length<=1){
//                    System.out.println(elements[0]);
//                    News err = new News();
//                    err.setText(elements[0]);
//                    err.setItemid(0);
//                    return err;
//                }

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

        JavaPairRDD<String, PositionalWord> singleWord = text.flatMap(new FlatMapFunction<Tuple2<String, Integer>, PositionalWord>() {
            public Iterator<PositionalWord> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {

                Iterator<String> s = Arrays.asList(stringIntegerTuple2._1
                        .replaceAll("(\\pP)|(\\pS)|(\\s)|(null)|(amp)|(quot)", " ").split(" ")).iterator();
                int itemid = stringIntegerTuple2._2;
                String word = "";
                int frequency = 1;
                int i = 0;//position
                String regex = "(\\d+)";
                Pattern p = Pattern.compile(regex);
                List<PositionalWord> list = new ArrayList<PositionalWord>();

                while (s.hasNext()) {
                    Hashtable<Integer, ArrayList<Integer>> hash = new Hashtable<Integer, ArrayList<Integer>>();

                    i++;
                    word = s.next();
                    if (word.equals("") | p.matcher(word).find()) {
                        continue;
                    }

                    if (hash.containsKey(itemid)) {
                        hash.get(itemid).add(i);
                    } else {
                        ArrayList<Integer> arr = new ArrayList<Integer>();
                        arr.add(i);
                        hash.put(itemid, arr);
                    }
                    PositionalWord pw = new PositionalWord();
                    pw.setKey(word);
                    pw.setFrequency(frequency);
                    pw.setList(hash);
                    list.add(pw);
                }
                return list.iterator();
            }
        }).mapToPair(new PairFunction<PositionalWord, String, PositionalWord>() {
            public Tuple2<String, PositionalWord> call(PositionalWord positionalWord) throws Exception {

                return new Tuple2<String, PositionalWord>(positionalWord.getKey(), positionalWord);
            }
        }).reduceByKey(new Function2<PositionalWord, PositionalWord, PositionalWord>() {
            public PositionalWord call(PositionalWord positionalWord, PositionalWord positionalWord2) throws Exception {
                int frequency = positionalWord.getFrequency()+positionalWord2.getFrequency();

                for(Map.Entry<Integer, ArrayList<Integer>> entry : positionalWord.getList().entrySet()){
                    if(!positionalWord2.getList().containsKey(entry.getKey())){
                        positionalWord2.getList().put(entry.getKey(), entry.getValue());
                    }
                }

                positionalWord2.setFrequency(frequency);

                return positionalWord2;
            }
        }).repartition(20).sortByKey();

        singleWord.saveAsTextFile(outDirectory+"/positionalIndexSequence");

        System.out.println("Success!");

//        JavaPairRDD<String, Tuple2<Integer,Hashtable<Integer,ArrayList<Integer>>>> singleWord = text.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Integer>, String, Tuple2<Integer, Hashtable<Integer, ArrayList<Integer>>>>() {
//            public Iterator<Tuple2<String, Tuple2<Integer, Hashtable<Integer, ArrayList<Integer>>>>> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                List<Tuple2<String,Tuple2<Integer, Hashtable<Integer, ArrayList<Integer>>>>> list = new ArrayList<Tuple2<String, Tuple2<Integer, Hashtable<Integer, ArrayList<Integer>>>>>();
//                Iterator<String> s = Arrays.asList(stringIntegerTuple2._1
//                        .replaceAll("(\\pP)|(\\pS)|(\\s)|(null)|(amp)|(quot)", " ").split(" ")).iterator();
//                int itemid = stringIntegerTuple2._2;
//                ArrayList<Integer> pos = new ArrayList<Integer>();
//                Hashtable<Integer, ArrayList<Integer>> hash = new Hashtable<Integer, ArrayList<Integer>>();
//                String word = "";
//                int frequency = 1;
//                int i = 0;//position
//                String regex = "(\\d+)";
//                Pattern p = Pattern.compile(regex);
//
//                while (s.hasNext()) {
//
//                    i++;
//                    word = s.next();
//                    if (word.equals("") | p.matcher(word).find()) {
//                        i++;
//                        continue;
//                    }
//
//                    if (hash.containsKey(itemid)) {
//                        hash.get(itemid).add(i);
//                    } else {
//                        ArrayList<Integer> arr = new ArrayList<Integer>();
//                        arr.add(i);
//                        hash.put(itemid, arr);
//                    }
//
//                    list.add(new Tuple2<String, Tuple2<Integer, Hashtable<Integer, ArrayList<Integer>>>>
//                            (word, new Tuple2<Integer, Hashtable<Integer, ArrayList<Integer>>>(frequency, hash)));
//                }
//                return list.iterator();
//            }
//        }).reduceByKey(new Function2<Tuple2<Integer, Hashtable<Integer, ArrayList<Integer>>>, Tuple2<Integer, Hashtable<Integer, ArrayList<Integer>>>, Tuple2<Integer, Hashtable<Integer, ArrayList<Integer>>>>() {
//            public Tuple2<Integer, Hashtable<Integer, ArrayList<Integer>>> call(Tuple2<Integer, Hashtable<Integer, ArrayList<Integer>>> integerHashtableTuple2, Tuple2<Integer, Hashtable<Integer, ArrayList<Integer>>> integerHashtableTuple22) throws Exception {
//                int frequency = integerHashtableTuple2._1 + integerHashtableTuple22._1;
////                Hashtable<Integer, ArrayList<Integer>> hash = new Hashtable<Integer, ArrayList<Integer>>();
//
//
//                //merge
//                for(Map.Entry<Integer, ArrayList<Integer>> entry : integerHashtableTuple2._2.entrySet()){
//                    if(!integerHashtableTuple22._2.containsKey(entry.getKey())) {
//                        integerHashtableTuple22._2.put(entry.getKey(), entry.getValue());
//                    }
//                }
//
//                return new Tuple2<Integer, Hashtable<Integer, ArrayList<Integer>>>(frequency, integerHashtableTuple22._2);
//            }
//        }).repartition(20).sortByKey();
//
//        singleWord.saveAsTextFile(outDirectory+"/positionalIndexSequence");
//
//        System.out.println("Success!");
    }
}
