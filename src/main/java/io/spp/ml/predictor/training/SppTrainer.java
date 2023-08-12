package io.spp.ml.predictor.training;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;

import io.spp.ml.predictor.dao.SppMLTrainingDao;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class SppTrainer
{
    @Autowired
    private SppMLTrainingDao sppMLTrainingDao;

    private ExecutorService executorService;

    public SppTrainer()
    {
        executorService = Executors.newWorkStealingPool();
    }
    
    public void train()
    {
        Dataset<Row> securityExchangeCodesDf = sppMLTrainingDao.loadSecurityExchangeCodes().select("exchangeCode").cache();
        List<Row> exchangeCodesList = securityExchangeCodesDf.collectAsList();
        Dataset<Row> indexReturnDf = sppMLTrainingDao.loadIndexReturns();
        Dataset<Row> securityReturnDf = sppMLTrainingDao.loadSecurityReturns(exchangeCodesList);
        Dataset<Row> securityAnalyticsDf = sppMLTrainingDao.loadSecurityAnalytics(exchangeCodesList);
        Dataset<Row> securityTrainingPScoreDf = sppMLTrainingDao.loadSecurityTrainingPScore(exchangeCodesList);

        Dataset<Row> securityDataAll = securityReturnDf.select(new Column("exchange"), new Column("exchangeCode"), new Column("isin"), new Column("date"), new Column("returns.90D.return").as("securityReturns90D"))
                                                                              .join(securityAnalyticsDf, new String[] {"exchange", "exchangeCode", "isin", "date"})
                                                                              .join(securityTrainingPScoreDf.select(new Column("exchange"), new Column("index"), new Column("exchangeCode"), new Column("isin"), new Column("date"), new Column("trainingPScore.90D.pScore").as("trainingPScore90D")), new String[] {"exchange", "exchangeCode", "isin", "date"})
                                                                              .join(indexReturnDf.select(new Column("exchange"), new Column("index"), new Column("date"), new Column("returns.90D.return").as("indexReturns90D")), new String[] {"exchange", "index", "date"})
                                                                              .orderBy("date");


        List<CompletableFuture<Dataset<Row>>> futures = new ArrayList<>();

        exchangeCodesList.forEach(r->{
            CompletableFuture<Dataset<Row>> future = CompletableFuture.supplyAsync(new SppTrainingTask(r.getString(0), securityDataAll), executorService);
            futures.add(future);
        });
        
        CompletableFuture<Void> allDone = CompletableFuture.allOf(futures.toArray(new CompletableFuture[] {}));
        while(!allDone.isDone())
        {
            try
            {
                Thread.sleep(1000);
            }
            catch (InterruptedException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
        futures.forEach(f->{
            
            try 
            {
                f.get();
            } catch(ExecutionException | InterruptedException e)
            {
                log.error(e);
            }
            
        });
    }
        
        
        
}
