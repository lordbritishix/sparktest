package com.quitevis.sparktest;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

@Slf4j
public class App {
    private static JavaSparkContext getContext() {
        SparkConf conf =
                new SparkConf()
                        .setAppName("sparkTest")
                        .setMaster("spark://sparkmaster:7077")
                        .setJars(new String[]{"/home/lordbritishix/src/sparkTest/target/sparkTest-1.0-SNAPSHOT.jar"});

        return new JavaSparkContext(conf);
    }
    public static void main( String[] args ) {
//        example1();
//        example2();
//        example3();
        example4();
    }

    /**
     * Example 1: Add fruit quantity of similar fruits using tuple2
     *
     * Expected output:
     * Apple: 7
     * Banana: 4
     * Orange: 6
     */
    private static void example1() {
        JavaSparkContext context = getContext();
        JavaPairRDD<String, Integer> rdd1 = context.parallelizePairs(
                ImmutableList.of(
                        new Tuple2<>("Apple", 5), new Tuple2<>("Apple", 2),
                        new Tuple2<>("Banana", 3), new Tuple2<>("Banana", 1),
                        new Tuple2<>("Orange", 6)));
        log.info("@@@@@@@ RDD1: {}", rdd1.collect().toString());

        JavaPairRDD<String, Integer> rdd2 = rdd1.reduceByKey((x, y) -> x + y);
        log.info("@@@@@@@ RDD2: {}", rdd2.collect().toString());
    }

    /**
     * Example 2: Example 1 reload - using map as input
     *
     * Expected output:
     * Apple: 7
     * Orange: 6
     */
    private static void example2() {
        JavaSparkContext context = getContext();

        JavaRDD<String> rdd1 = context.parallelize(
                ImmutableList.of("Apple", "Apple", "Apple", "Apple", "Apple", "Apple", "Apple",
                        "Banana", "Banana", "Banana", "Banana",
                        "Orange", "Orange", "Orange", "Orange", "Orange", "Orange"));
        log.info("@@@@@@@ RDD1: {}", rdd1.collect().toString());

        JavaPairRDD<String, Integer> rdd2 = rdd1.mapToPair((key) -> new Tuple2<>(key, 1));
        log.info("@@@@@@@ RDD2: {}", rdd2.collect().toString());

        JavaPairRDD<String, Integer> rdd3 = rdd2.reduceByKey((val1, val2) -> val1 + val2);
        log.info("@@@@@@@ RDD3: {}", rdd3.collect().toString());

        JavaPairRDD<String, Integer> rdd4 = rdd3.filter((tuple) -> tuple._2() > 4);
        log.info("@@@@@@@ RDD4: {}", rdd4.collect().toString());
    }

    /**
     * Example 3: Example 2 reload - abridged
     *
     * Expected output:
     * Apple
     * Orange
     */
    private static void example3() {
        JavaSparkContext context = getContext();

        List<String> processedList =
                context.parallelize(
                ImmutableList.of("Apple", "Apple", "Apple", "Apple", "Apple", "Apple", "Apple",
                                "Banana", "Banana", "Banana", "Banana",
                                "Orange", "Orange", "Orange", "Orange", "Orange", "Orange"))
                .mapToPair((key) -> new Tuple2<>(key, 1))
                .reduceByKey((val1, val2) -> val1 + val2)
                .filter((tuple) -> tuple._2() > 4)
                .keys()
                .collect();

        log.info("@@@@@@@ RDD: {}", processedList.toString());
    }

    /**
     * Example 4: Find the average score for all tests for each person.
     * Only consider the highest score per test
     *
     * Expected output:
     * Myrcella: (10 + 6) / 2 = 8
     * Joffrey: (10 + 10) / 2 = 10
     * Tommen: (10 + 10) / 2 = 10
     * Cersei: (2 + 2 + 2) / 3 = 2
     */
    private static void example4() {
        JavaSparkContext context = getContext();

        JavaPairRDD<String, Tuple2<String, Integer>> rdd1 = context.parallelizePairs(
                ImmutableList.of(
                        new Tuple2<>("Myrcella", new Tuple2<>("Test1", 10)),     //
                        new Tuple2<>("Myrcella", new Tuple2<>("Test1", 8)),
                        new Tuple2<>("Myrcella", new Tuple2<>("Test2", 6)),      //
                        new Tuple2<>("Joffrey", new Tuple2<>("Test1", 7)),
                        new Tuple2<>("Joffrey", new Tuple2<>("Test1", 6)),
                        new Tuple2<>("Joffrey", new Tuple2<>("Test1", 10)),     //
                        new Tuple2<>("Joffrey", new Tuple2<>("Test2", 10)),     //
                        new Tuple2<>("Joffrey", new Tuple2<>("Test2", 8)),
                        new Tuple2<>("Tommen", new Tuple2<>("Test1", 10)),      //
                        new Tuple2<>("Tommen", new Tuple2<>("Test2", 10)),      //
                        new Tuple2<>("Cersei", new Tuple2<>("Test1", 2)),       //
                        new Tuple2<>("Cersei", new Tuple2<>("Test2", 2)),       //
                        new Tuple2<>("Cersei", new Tuple2<>("Test3", 2))        //
                ));
        log.info("@@@@@@@ RDD1: {}", rdd1.collect().toString());

        //Create compound key, composed of name and test id
        JavaPairRDD<String, Tuple2<String, Integer>> rdd2 = rdd1.mapToPair(stringTuple2Tuple2 -> {
            String name = stringTuple2Tuple2._1();
            String testId = stringTuple2Tuple2._2()._1();
            String newName = name + "_" + testId;
            Tuple2<String, Tuple2<String, Integer>> map =
                    new Tuple2<String, Tuple2<String, Integer>>(newName, stringTuple2Tuple2._2());

            return map;
        });

        //Reduce by only picking greatest score
        JavaPairRDD<String, Tuple2<String, Integer>> rdd3 =
            rdd2.reduceByKey((stringIntegerTuple2, stringIntegerTuple22) ->
                    stringIntegerTuple2._2() > stringIntegerTuple22._2() ? stringIntegerTuple2 : stringIntegerTuple22);

        //Transform back key to only name
        JavaPairRDD<String, Tuple2<String, Integer>> rdd4 =
            rdd3.mapToPair(stringTuple2Tuple2 -> {
                String newName = stringTuple2Tuple2._1().split("_")[0];
                return new Tuple2<>(newName, stringTuple2Tuple2._2());
            });

        log.info("@@@@@@ RDD4: {}", rdd4.collect().toString());

        //Start to compute for average, put 1 in all records
        JavaPairRDD<String, Tuple2<Tuple2<String, Integer>, Integer>> rdd5 =
                rdd4.mapValues(stringIntegerTuple2 -> new Tuple2(stringIntegerTuple2, 1));

        //Reduce by name, sum up all the 1's and all the scores
        JavaPairRDD<String, Tuple2<Tuple2<String, Integer>, Integer>> rdd6 = rdd5.reduceByKey((tuple2IntegerTuple2, tuple2IntegerTuple22) -> {
            Tuple2<String, Integer> inner = new Tuple2<String, Integer>("", tuple2IntegerTuple2._1()._2() + tuple2IntegerTuple22._1()._2());
            int counter = tuple2IntegerTuple2._2() + tuple2IntegerTuple22._2();

            Tuple2<Tuple2<String, Integer>, Integer> ret =
                    new Tuple2<Tuple2<String, Integer>, Integer>(inner, counter);

            return ret;
        });

        log.info("@@@@@@ RDD6: {}", rdd6.collect().toString());

        //Finally, map to the desired output, and divde the sum of all 1's from the sum of all scores
        JavaPairRDD<String, Integer> rdd7 = rdd6.mapToPair(stringTuple2Tuple2 -> {
            Tuple2<String, Integer> ret =
                    new Tuple2<>(stringTuple2Tuple2._1(), stringTuple2Tuple2._2()._1()._2() / stringTuple2Tuple2._2()._2());
            return ret;
        });

        log.info("@@@@@@ RDD7: {}", rdd7.collect().toString());
    }
}
