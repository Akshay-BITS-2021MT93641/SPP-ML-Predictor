package io.spp.ml.predictor;

import java.io.IOException;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import io.spp.ml.predictor.config.SppConfigProperties;
import io.spp.ml.predictor.dao.QueryFiles;
import io.spp.ml.predictor.dao.QueryHolder;

public class SubmitToSpark
{
    
    static {
        QueryFiles.load();
    }

    public static void main(String[] args) throws IOException
    {
        String configFile = ArrayUtils.isNotEmpty(args) && StringUtils.isNotBlank(args[0]) ? args[0] : "spp-local.properties";
        SppConfigProperties.load(configFile);
        
        SparkSession spark = SparkSession.builder()
                                                                .appName("SPP")
                                                                .config(SppConfigProperties.getPropertiesMap())
                                                                .getOrCreate();
        
        String securityReturnsCollection = QueryHolder.getQuery(QueryFiles.SPP_STOCK_DATA_MQL, "loadSecurityReturnsCollectionName");
        String securityReturnsMql = QueryHolder.getQuery(QueryFiles.SPP_STOCK_DATA_MQL, "loadSecurityReturnsMql");
        
        Dataset<Row> df = spark.read().format("mongodb")
                                            .option("spark.mongodb.read.collection", securityReturnsCollection)
                                            .option("spark.mongodb.read.aggregation.pipeline", securityReturnsMql)
                                            .load();
        
        df.printSchema();
        df.show();
        
        
        
    }

}
