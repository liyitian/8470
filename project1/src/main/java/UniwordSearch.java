import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;
// Import factory methods provided by DataTypes.
import org.apache.spark.sql.types.DataTypes;
// Import StructType and StructField
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
// Import Row.
import org.apache.spark.sql.Row;
// Import RowFactory.
import org.apache.spark.sql.RowFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created by tiger on 4/12/16.
 */
public class UniwordSearch
{

    private SparkConf config;
    private JavaSparkContext context;
    private SQLContext sqlContext;

    public UniwordSearch()
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
        UniwordSearch us = new UniwordSearch();
//        final Loader loader = new Loader();
        // transform the data into block entries

        JavaRDD<String> rdd = null;
        String requiredWord = "";
        if(args.length > 0){
            requiredWord = args[0];
        }

        // default parameters
        String inDirectory = "output/uniwordSequence";
        String outDirectory = "output/uniwordoutput";

                JavaRDD<String> tmp = us.loadData(inDirectory + "/part-*");

                if (rdd == null) rdd = tmp;
                else rdd = rdd.union(tmp);


// The schema is encoded in a string

// Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<StructField>();

        fields.add(DataTypes.createStructField("word", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("frequency", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("itemid", DataTypes.StringType, true));

        StructType schema = DataTypes.createStructType(fields);

// Convert records of the RDD (people) to Rows.
        JavaRDD<Row> rowRDD = rdd.map(
                new Function<String, Row>() {
                    public Row call(String record) throws Exception {
                        String[] fields = record.replaceAll("(\\()|(\\[)|(\\]\\))", "").split("(,)|(,\\[)|(,\\])");
                        StringBuilder s = new StringBuilder();
                        for(int i = 2; i < fields.length; i++){
                            if(i == 2) {
                                s.append(fields[i].trim());
                            }else{
                                s.append("," + fields[i].trim());
                            }
                        }

                        return RowFactory.create(fields[0].trim(), Integer.parseInt(fields[1].trim()), s.toString());
                    }
                });

// Apply the schema to the RDD.

        Dataset<Row> peopleDataFrame = us.sqlContext.createDataFrame(rowRDD, schema);

// Register the DataFrame as a table.
        peopleDataFrame.registerTempTable("uniword");

// SQL can be run over RDDs that have been registered as tables.
        Dataset<Row> results = us.sqlContext.sql("SELECT * FROM uniword WHERE word LIKE '%"+requiredWord+"%'");

// The results of SQL queries are DataFrames and support all the normal RDD operations.
// The columns of a row in the result can be accessed by ordinal.
        List<String> names = results.javaRDD().map(new Function<Row, String>() {
            public String call(Row row) {
                return "Word: " + row.getString(0) +
                        ",Frequency: " + row.getInt(1) +
                        ",ItemId: " + row.getString(2)
                        ;
            }
        }).collect();

        if(names.isEmpty())
            System.out.println("Not Found!");
        else
            System.out.println(names.toString());

        System.out.println("Success!");
    }
}