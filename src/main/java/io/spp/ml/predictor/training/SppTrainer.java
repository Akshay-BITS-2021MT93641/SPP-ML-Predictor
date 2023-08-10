package io.spp.ml.predictor.training;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;

import io.spp.ml.predictor.dao.SppMLTrainingDao;

public class SppTrainer
{
    @Autowired
    private SppMLTrainingDao sppMLTrainingDao;
    
    public void train()
    {
        Dataset<Row> securityExchangeCodesDf = sppMLTrainingDao.loadSecurityExchangeCodes().select("exchangeCode").cache();
        List<Row> exchangeCodesList = securityExchangeCodesDf.collectAsList();
        Dataset<Row> indexReturnDf = sppMLTrainingDao.loadIndexReturns();
        Dataset<Row> securityReturnDf = sppMLTrainingDao.loadSecurityReturns(exchangeCodesList);
        Dataset<Row> securityAnalyticsDf = sppMLTrainingDao.loadSecurityAnalytics(exchangeCodesList);
        Dataset<Row> securityTrainingPScoreDf = sppMLTrainingDao.loadSecurityTrainingPScore(exchangeCodesList);
        
        Dataset<Row> securityDataAll = securityReturnDf.withColumnRenamed("returns", "securityReturns")
                                                                              .join(securityAnalyticsDf, new String[] {"exchange", "exchangeCode", "isin", "date"})
                                                                              .join(indexReturnDf.withColumnRenamed("returns", "indexReturns"), new String[] {"exchange", "date"})
                                                                              .join(securityTrainingPScoreDf, new String[] {"exchange", "exchangeCode", "isin", "date"})
                                                                              .orderBy("date");
        
        securityDataAll.printSchema();
        
    }
        
        
        
}
