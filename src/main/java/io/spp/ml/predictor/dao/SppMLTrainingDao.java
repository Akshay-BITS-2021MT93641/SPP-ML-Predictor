package io.spp.ml.predictor.dao;

import static io.spp.ml.predictor.SppGlobalContext.exchangeCodeKey;
import static io.spp.ml.predictor.SppGlobalContext.exchangeKey;
import static io.spp.ml.predictor.SppGlobalContext.indexKey;
import static io.spp.ml.predictor.SppGlobalContext.trainingEndDateKey;
import static io.spp.ml.predictor.SppGlobalContext.trainingStartDateKey;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;

import io.fop.context.impl.ApplicationContextHolder;
import io.spp.ml.predictor.SppGlobalContext;

public class SppMLTrainingDao
{  
    @Autowired
    private SparkSession spark;
    
    public Dataset<Row> loadSecurityExchangeCodes()
    {
        SppGlobalContext sppGlobalContext = (SppGlobalContext)ApplicationContextHolder.getGlobalContext(SppGlobalContext.class.getSimpleName());
        String exchange = sppGlobalContext.fetch(exchangeKey);
        String exchangeCodeGlobal = sppGlobalContext.fetch(exchangeCodeKey);
        String securityCodesCollection = QueryHolder.getQuery(QueryFiles.SPP_STOCK_DATA_MQL, "loadSecurityExchangeCodesCollectionName");
        String securityCodesMql = QueryHolder.getQuery(QueryFiles.SPP_STOCK_DATA_MQL, "loadSecurityExchangeCodesMql");
        securityCodesMql = String.format(securityCodesMql, exchange, (StringUtils.isNotBlank(exchangeCodeGlobal) ? ", exchangeCode:\""+exchangeCodeGlobal + "\"" : ""));
        
        return spark.read().format("mongodb")
                .option("spark.mongodb.read.collection", securityCodesCollection)
                .option("spark.mongodb.read.aggregation.pipeline", securityCodesMql)
                .load();
    }
    
    public Dataset<Row> loadSecurityReturns(String exchangeCode)
    {
        SppGlobalContext sppGlobalContext = (SppGlobalContext)ApplicationContextHolder.getGlobalContext(SppGlobalContext.class.getSimpleName());
        String startDateStr = sppGlobalContext.fetch(trainingStartDateKey);
        String endDateStr = sppGlobalContext.fetch(trainingEndDateKey);
        String exchange = sppGlobalContext.fetch(exchangeKey);
        
        String securityReturnsCollection = QueryHolder.getQuery(QueryFiles.SPP_STOCK_DATA_MQL, "loadSecurityReturnsCollectionName");
        String securityReturnsMql = QueryHolder.getQuery(QueryFiles.SPP_STOCK_DATA_MQL, "loadSecurityReturnsMql");
        securityReturnsMql = String.format(securityReturnsMql, exchange, startDateStr, endDateStr, exchangeCode);
        
        return spark.read().format("mongodb")
                    .option("spark.mongodb.read.collection", securityReturnsCollection)
                    .option("spark.mongodb.read.aggregation.pipeline", securityReturnsMql)
                    .load();
    }
    
    public Dataset<Row> loadIndexReturns()
    {
        SppGlobalContext sppGlobalContext = (SppGlobalContext)ApplicationContextHolder.getGlobalContext(SppGlobalContext.class.getSimpleName());
        String startDateStr = sppGlobalContext.fetch(trainingStartDateKey);
        String endDateStr = sppGlobalContext.fetch(trainingEndDateKey);
        String exchange = sppGlobalContext.fetch(exchangeKey);
        String index = sppGlobalContext.fetch(indexKey);
        
        String indexReturnsCollection = QueryHolder.getQuery(QueryFiles.SPP_STOCK_DATA_MQL, "loadIndexReturnsCollectionName");
        String indexReturnsMql = QueryHolder.getQuery(QueryFiles.SPP_STOCK_DATA_MQL, "loadIndexReturnsMql");
        indexReturnsMql = String.format(indexReturnsMql, exchange, index, startDateStr, endDateStr);
        
        return spark.read().format("mongodb")
                    .option("spark.mongodb.read.collection", indexReturnsCollection)
                    .option("spark.mongodb.read.aggregation.pipeline", indexReturnsMql)
                    .load();
    } 
    
    public Dataset<Row> loadSecurityAnalytics(String exchangeCode)
    {
        SppGlobalContext sppGlobalContext = (SppGlobalContext)ApplicationContextHolder.getGlobalContext(SppGlobalContext.class.getSimpleName());
        String startDateStr = sppGlobalContext.fetch(trainingStartDateKey);
        String endDateStr = sppGlobalContext.fetch(trainingEndDateKey);
        String exchange = sppGlobalContext.fetch(exchangeKey);
        
        String securityReturnsCollection = QueryHolder.getQuery(QueryFiles.SPP_STOCK_DATA_MQL, "loadSecurityAnalyticsCollectionName");
        String securityReturnsMql = QueryHolder.getQuery(QueryFiles.SPP_STOCK_DATA_MQL, "loadSecurityAnalyticsMql");
        securityReturnsMql = String.format(securityReturnsMql, exchange, startDateStr, endDateStr, exchangeCode);
        
        return spark.read().format("mongodb")
                    .option("spark.mongodb.read.collection", securityReturnsCollection)
                    .option("spark.mongodb.read.aggregation.pipeline", securityReturnsMql)
                    .load();
    }
    
    public Dataset<Row> loadSecurityTrainingPScore(String exchangeCode)
    {
        SppGlobalContext sppGlobalContext = (SppGlobalContext)ApplicationContextHolder.getGlobalContext(SppGlobalContext.class.getSimpleName());
        String startDateStr = sppGlobalContext.fetch(trainingStartDateKey);
        String endDateStr = sppGlobalContext.fetch(trainingEndDateKey);
        String exchange = sppGlobalContext.fetch(exchangeKey);
        String index = sppGlobalContext.fetch(indexKey);
        
        String securityTrainingPScoreCollection = QueryHolder.getQuery(QueryFiles.SPP_STOCK_DATA_MQL, "loadSecurityTrainingPScoreCollectionName");
        String securityTrainingPScoreMql = QueryHolder.getQuery(QueryFiles.SPP_STOCK_DATA_MQL, "loadSecurityTrainingPScoreMql");
        securityTrainingPScoreMql = String.format(securityTrainingPScoreMql, exchange, index, startDateStr, endDateStr, exchangeCode);
        
        return spark.read().format("mongodb")
                    .option("spark.mongodb.read.collection", securityTrainingPScoreCollection)
                    .option("spark.mongodb.read.aggregation.pipeline", securityTrainingPScoreMql)
                    .load();
    }
}
