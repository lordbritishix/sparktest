package com.quitevis.sparktest;

import com.google.common.collect.ImmutableList;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ) {
        SparkConf conf =
                new SparkConf().setAppName("sparkTest").setMaster("spark://sparkmaster:7077");
        JavaSparkContext context = new JavaSparkContext(conf);
        context.addJar("/home/lordbritishix/src/sparkTest/target/sparkTest-1.0-SNAPSHOT.jar");

        JavaRDD<Integer> numberRDD = context.parallelize(ImmutableList.of(1, 2, 3, 4, 5));
        Integer sum = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) throws Exception {
                System.out.println("@@@@@@ Calling x and y for " + x + ", " + y);

                return x + y;
            }
        });

        System.out.println("@@@@ " + sum);
    }
}
