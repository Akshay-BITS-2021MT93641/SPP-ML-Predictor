package io.spp.ml.predictor.dao;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

public class SppMLTrainingDao
{
    @Autowired
    private Environment environment;
    
    @Autowired
    private SparkSession spark;
    
    public Dataset<Row> loadSecurityReturns()
    {
        String startDateStr = environment.getProperty("spp.training.startDate");
        String endDateStr = environment.getProperty("spp.training.endDate");
        
        String securityReturnsCollection = QueryHolder.getQuery(QueryFiles.SPP_STOCK_DATA_MQL, "loadSecurityReturnsCollectionName");
        String securityReturnsMql = QueryHolder.getQuery(QueryFiles.SPP_STOCK_DATA_MQL, "loadSecurityReturnsMql");
        securityReturnsMql = String.format(securityReturnsMql, startDateStr, endDateStr);
        
        return spark.read().format("mongodb")
                    .option("spark.mongodb.read.collection", securityReturnsCollection)
                    .option("spark.mongodb.read.aggregation.pipeline", securityReturnsMql)
                    .load();
    }
    
    public Dataset<Row> loadIndexReturns()
    {
        String startDateStr = environment.getProperty("spp.training.startDate");
        String endDateStr = environment.getProperty("spp.training.endDate");
        
        String indexReturnsCollection = QueryHolder.getQuery(QueryFiles.SPP_STOCK_DATA_MQL, "loadIndexReturnsCollectionName");
        String indexReturnsMql = QueryHolder.getQuery(QueryFiles.SPP_STOCK_DATA_MQL, "loadIndexReturnsMql");
        indexReturnsMql = String.format(indexReturnsMql, startDateStr, endDateStr);
        
        return spark.read().format("mongodb")
                    .option("spark.mongodb.read.collection", indexReturnsCollection)
                    .option("spark.mongodb.read.aggregation.pipeline", indexReturnsMql)
                    .load();
    } 
    
    public Dataset<Row> loadSecurityAnalytics()
    {
        String startDateStr = environment.getProperty("spp.training.startDate");
        String endDateStr = environment.getProperty("spp.training.endDate");
        
        String securityReturnsCollection = QueryHolder.getQuery(QueryFiles.SPP_STOCK_DATA_MQL, "loadSecurityAnalyticsCollectionName");
        String securityReturnsMql = QueryHolder.getQuery(QueryFiles.SPP_STOCK_DATA_MQL, "loadSecurityAnalyticsMql");
        securityReturnsMql = String.format(securityReturnsMql, startDateStr, endDateStr);
        
        return spark.read().format("mongodb")
                    .option("spark.mongodb.read.collection", securityReturnsCollection)
                    .option("spark.mongodb.read.aggregation.pipeline", securityReturnsMql)
                    .load();
    }
    
    public Dataset<Row> loadSecurityTrainingPScore()
    {
        String startDateStr = environment.getProperty("spp.training.startDate");
        String endDateStr = environment.getProperty("spp.training.endDate");
        
        String securityTrainingPScoreCollection = QueryHolder.getQuery(QueryFiles.SPP_STOCK_DATA_MQL, "loadSecurityTrainingPScoreCollectionName");
        String securityTrainingPScoreMql = QueryHolder.getQuery(QueryFiles.SPP_STOCK_DATA_MQL, "loadSecurityTrainingPScoreMql");
        securityTrainingPScoreMql = String.format(securityTrainingPScoreMql, startDateStr, endDateStr);
        
        return spark.read().format("mongodb")
                    .option("spark.mongodb.read.collection", securityTrainingPScoreCollection)
                    .option("spark.mongodb.read.aggregation.pipeline", securityTrainingPScoreMql)
                    .load();
    }
}
