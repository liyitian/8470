import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by tiger on 4/12/16.
 */
public class PositionSearch
{

    private SparkConf config;
    private JavaSparkContext context;
    private SQLContext sqlContext;

    public PositionSearch()
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

    public static void main(String[] args) {
        // set up and load file
        PositionSearch us = new PositionSearch();
        // transform the data into block entries

        JavaRDD<String> rdd = null;
        String word1 = "";
        String word2 = "";
        String delta = "";
        if(args.length > 0){
            word1 = args[0];
            word2 = args[1];
            delta = args[2];
        }

        // default parameters

        String inDirectory = "output/positionalIndexSequence";
        String outDirectory = "output/positionaloutput";

                JavaRDD<String> tmp = us.loadData(inDirectory + "/part-*");

                if (rdd == null) rdd = tmp;
                else rdd = rdd.union(tmp);


// The schema is encoded in a string

// Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<StructField>();

        fields.add(DataTypes.createStructField("word", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("frequency", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("list", DataTypes.StringType, true));

        StructType schema = DataTypes.createStructType(fields);

// Convert records of the RDD (people) to Rows.
        JavaRDD<Row> rowRDD = rdd.map(
                new Function<String, Row>() {
                    public Row call(String record) throws Exception {
                        String[] fields = record
                                .replaceAll("(\\()|(\\]\\))|(key=)|(list=)|(\\}\\))|(frequency=)|(')", "")
                                .split("(\\{)");
                        String[] front = fields[0].split(",");

                        return RowFactory.create(front[0], Integer.parseInt(front[2].trim()), fields[1]);
                    }
                });

// Apply the schema to the RDD.

        Dataset<Row> peopleDataFrame = us.sqlContext.createDataFrame(rowRDD, schema);

// Register the DataFrame as a table.
        peopleDataFrame.registerTempTable("positionalIndex");

// SQL can be run over RDDs that have been registered as tables.
        Dataset<Row> results1 = us.sqlContext.sql("SELECT * FROM positionalIndex WHERE word LIKE '"+ word1 +"'");
        Dataset<Row> results2 = us.sqlContext.sql("SELECT * FROM positionalIndex WHERE word LIKE '"+ word2 +"'");


        List<Row> list1 = results1.collectAsList();
        List<Row> list2 = results2.collectAsList();

        if((!list1.isEmpty())&&(!list2.isEmpty())) {

            //word,fre,id1=pos1 id2=pos2 id3=pos3...
            String[] s1 = list1.get(0).toString().split("\\],");
            String[] s2 = list2.get(0).toString().split("\\],");

            //word fre id1=pos1
            String[] t_s1 = s1[0].split(",");
            String key1 = t_s1[0].replaceAll("\\[","");
            String fre1 = t_s1[1];
            //id1 pos1,pos2
            String[] first1 = t_s1[2].split("(=\\[)|(,)");

            String[] t_s2 = s2[0].split(",");
            String key2 = t_s2[0].replaceAll("\\[","");
            String fre2 = t_s2[1];
            String[] first2 = t_s2[2].split("(=\\[)|(,)");

            StringBuilder sb = new StringBuilder();

            if(first1[0] == first2[0]){
                for(int i = 1;i < first1.length;i++){
                    for(int j = 1;j < first2.length;j++){
                        if(Integer.parseInt(first1[i])-Integer.parseInt(first2[j])==Integer.parseInt(delta))
                            sb.append(first1[0]);
                    }
                }
            }

            StringBuilder a = new StringBuilder();
            StringBuilder b = new StringBuilder();

            for (int i = 1; i < s1.length; i++) {
                //id pos1,pos2...
                String[] t_si = s1[i].split("(=\\[)|(,)");
                int id1 = Integer.parseInt(t_si[0].toString().trim());


                for (int j = 1; j < s2.length; j++) {
                    String[] t_sj = s2[j].split("(=\\[)|(,)");
                    int id2 = Integer.parseInt(t_sj[0].toString().trim());
                    int k1=1;
                    int k2=1;
                    int pos1 = 0,pos2 = 0;

                    if(id1 == id2){
                        for(;k1 < t_si.length;k1++){
                            pos1 = Integer.parseInt(t_si[k1]);
                            for(;k2 < t_sj.length;k2++){
                            pos2 = Integer.parseInt(t_sj[k2]);
                                if (Math.abs(pos1 - pos2) == Integer.parseInt(delta))
                                    sb.append(" "+id1);
                            }
                        }

                    }else continue;

                }
            }

           if(sb.toString().equals("")) sb.append("no value");
           System.out.println(sb.toString());

        }else{
            System.out.println("Not Found!");
        }
        System.out.println("Success!");
    }
}