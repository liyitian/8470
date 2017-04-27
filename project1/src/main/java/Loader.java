//---------------spark_import
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


//----------------xml_loader_import
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import com.cloudera.datascience.common.XmlInputFormat;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class Loader implements Serializable {
    private SparkConf config;
    private JavaSparkContext context;

        public Loader(){
            config = new SparkConf().setAppName("XML Reading");
            config.setMaster("local[4]")
                    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec")
                    .set("spark.hadoop.validateOutputSpecs", "false");
                    ;
             context = new JavaSparkContext(config);

        }

        public JavaRDD<String> loadData(String file)
    {
        return context.textFile(file);
    }

        public static JavaRDD<String> readFile( String path, String start_tag, String end_tag, JavaSparkContext sc){
        Configuration conf = new Configuration();
        conf.set(XmlInputFormat.START_TAG_KEY, start_tag);
        conf.set(XmlInputFormat.END_TAG_KEY, end_tag);

          JavaPairRDD<LongWritable, Text> input = sc.newAPIHadoopFile(path, XmlInputFormat.class,LongWritable.class, Text.class,
                    conf);
            JavaRDD<String> toString = input.map(new Function<Tuple2<LongWritable, Text>, String>() {
                public String call(Tuple2<LongWritable, Text> longWritableTextTuple2) throws Exception {

                    return longWritableTextTuple2._2.toString();
                }
            });

            return toString;
    }

    public static News extractFields(String tuple) {

        String value = tuple.replaceAll("\r\n", "");
        String[] v ={""};
        String[] propertites = {""};
        int itemid = 0;
        String title = "";
        String date = "";
        String headline = "";
        String byline = "";
        String dateline = "";
        String text = "";
        String metadata = "";
        List<String> countriesCodes = new ArrayList<String>();
        List<String> industriesCodes = new ArrayList<String>();
        List<String> topicsCodes = new ArrayList<String>();
        String tag = "title";
        News news = new News();



        if (value.contains("<" + tag + ">") && value.contains("</" + tag + ">")||value.contains("/>")) {

            v = value.split("<" + tag + ">");
            propertites = v[0].split("(\")");
            itemid = Integer.parseInt(propertites[1]);
            date = propertites[5];
            v = v[1].split("</" + tag + ">");
            title = v[0];

            news.setItemid(itemid);
            news.setDate(date);
            news.setTitle(title);

        }

        tag = "headline";
        if (v[1].contains("<" + tag + ">") && v[1].contains("</" + tag + ">")) {

            v = v[1].split("<" + tag + ">");
            v = v[1].split("</" + tag + ">");
            headline = v[0];
            news.setHeadline(headline);
        }

        tag = "byline";
        if (v[1].contains("<" + tag + ">") && v[1].contains("</" + tag + ">")) {

            v = v[1].split("<" + tag + ">");
            v = v[1].split("</" + tag + ">");
            byline = v[0];
            news.setByline(byline);
        }

        tag = "dateline";
        if (v[1].contains("<" + tag + ">") && v[1].contains("</" + tag + ">")) {

            v = v[1].split("<" + tag + ">");
            v = v[1].split("</" + tag + ">");
            dateline = v[0];
            news.setDateline(dateline);
        }

        tag = "text";
        if (v[1].contains("<" + tag + ">") && v[1].contains("</" + tag + ">")) {

            v = v[1].split("<" + tag + ">");
            v = v[1].split("</" + tag + ">");
            text = v[0];
            text = text.replaceAll("(<p>)|(</p>)|(&quot;)|(&amp;)|(\n)"," ");
            news.setText(text);
        }

        tag = "metadata";
        if (v[1].contains("<" + tag + ">") && v[1].contains("</" + tag + ">")) {

            v = v[1].split("<" + tag + ">");
            v = v[1].split("</" + tag + ">");
            metadata = v[0];
            news.setMetadata(metadata);
        }

        tag = "codes";
        if (v[0].contains("<" + tag ) && v[0].contains("</" + tag + ">")) {

            v = v[0].split("<" + tag);

            for (int j = 1; j < v.length; j++) {
//               if(!s.contains("codes")) continue;
               String[] t_s = v[j].split("<code");
               if(t_s[0].contains("countries")) {
                   int i = 0;
                   for(int k = 1; k < t_s.length; k++){
//                       if(!s1.contains("code")) continue;
                       String[] t_s1 = t_s[k].split("\"");
                       countriesCodes.add(t_s1[1]);
                       i++;
                   }

               }else if(t_s[0].contains("industries")){
                   int i = 0;
                   for(int k = 1; k < t_s.length; k++){
//                       if(!s1.contains("code")) continue;
                       String[] t_s1 = t_s[k].split("\"");
                       industriesCodes.add(t_s1[1]);
                       i++;
                   }
               }else if(t_s[0].contains("topics")){
                   int i = 0;
                   for(int k = 1; k < t_s.length; k++){
//                       if(!s1.contains("code")) continue;
                       String[] t_s1 = t_s[k].split("\"");
                       topicsCodes.add(t_s1[1]);
                       i++;
                   }
               }
            }
            news.setCountriescodes(countriesCodes);
            news.setIndustriescodes(industriesCodes);
            news.setTopicscodes(topicsCodes);
        }

        return news;
    }

    public static void main(String[] args) {


        Loader loaderContext = new Loader();


        JavaRDD<String> rdd = loaderContext.loadData("data/part-*,data1/part-*");

        rdd = rdd.union(rdd);

        rdd = rdd.repartition(20);

        rdd.saveAsTextFile("finalData");

    }


//        JavaRDD<String> pages = readFile("files/*/*.xml", "<newsitem", "</newsitem>", loaderContext.context); //calling function 1.1
//
//        JavaRDD<News> df = pages.map(new Function<String, News>() {
//            public News call(String v1) throws Exception {
//                News news;
//                news = extractFields(v1);
//                return news;
//            }
//        });
//        df = df.repartition(20);
//        df.saveAsTextFile("data");
//    }
    }



