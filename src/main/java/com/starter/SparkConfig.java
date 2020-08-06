package com.starter;

import com.starter.properties.SparkProperties;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;


import static com.starter.properties.Profiles.DEV;
import static com.starter.properties.Profiles.PROD;


@Configuration
@EnableConfigurationProperties(SparkProperties.class)
public class SparkConfig
{


    @Autowired
    private SparkProperties sparkProperties;


    @Bean
    @Profile(PROD)
    public SparkSession sparkSessionProd()
    {
        SparkSession session = SparkSession.builder().master(sparkProperties.getProdMaster()).appName(sparkProperties.getProdAppName()).getOrCreate();
        session.sparkContext().setLogLevel("ERROR");
        return session;
    }

    @Bean
    @Profile(DEV)
    public SparkSession sparkSessionDev()
    {
        SparkSession session = SparkSession.builder().master(sparkProperties.getDevMaster()).appName(sparkProperties.getDevMaster()).getOrCreate();
        session.sparkContext().setLogLevel("ERROR");
        return session;
    }

    @Bean
    public SQLContext sqlContext(SparkContext sc)
    {
        return new SQLContext(sc);
    }


    @Bean
    public JavaSparkContext javaSparkContext(SparkContext sc)
    {
        return new JavaSparkContext(sc);
    }

    @Bean
    public SparkContext sc(SparkConf conf)
    {
        return SparkContext.getOrCreate(conf);
    }

    @Profile(PROD)
    @Bean
    public SparkConf sparkConfProd()
    {
        return new SparkConf().setAppName(sparkProperties.getProdAppName()).setMaster(sparkProperties.getProdMaster());
    }

    @Profile(DEV)
    @Bean
    public SparkConf sparkConfDev()
    {
        return new SparkConf().setAppName(sparkProperties.getDevAppName()).setMaster(sparkProperties.getDevMaster());
    }

    @Bean
    public StreamingContext streamingContext(SparkConf conf)
    {
        return new StreamingContext(conf, Duration.apply(sparkProperties.getBatchDuration()*1000));
    }



//    @Bean
//    public JavaSparkContext javaSparkContext() {
//        return new JavaSparkContext(sparkSession().sparkContext());
//    }


//    @Bean
//    public Broadcast<Map<String, Integer>> techRateMap() {
//        Map<String, Integer> map = new HashMap<>();
//        map.put("spring", 5);
//        map.put("spark", 5);
//        map.put("groovy", 5);
//        map.put("hadoop", 2);
//        map.put("yarn", 1);
//        return javaSparkContext().broadcast(map);
//    }
}
