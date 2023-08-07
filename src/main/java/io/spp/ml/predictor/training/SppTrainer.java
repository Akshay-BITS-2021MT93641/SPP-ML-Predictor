package io.spp.ml.predictor.training;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import io.spp.ml.predictor.dao.SppMLTrainingDao;

public class SppTrainer implements ApplicationContextAware
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
        Map<String, CompletableFuture<?>> futures = new HashMap<>();
        
        Dataset<Row> securityExchangeCodesDf = sppMLTrainingDao.loadSecurityExchangeCodes();
        
        securityExchangeCodesDf.toLocalIterator().forEachRemaining(r->{
            
            String exchangeCode = r.getString(0);
            SppTrainingTask sppTrainingTask = getSppTrainingTask(exchangeCode);
            CompletableFuture<?> future = CompletableFuture.runAsync(sppTrainingTask, executorService);
            futures.put(exchangeCode, future);
            
        });
        
//        securityExchangeCodesDf.select("exchangeCode")
//        .foreach(r->{
//            
//            String exchangeCode = r.getString(0);
//            SppTrainingTask sppTrainingTask = getSppTrainingTask(exchangeCode);
//            CompletableFuture<?> future = CompletableFuture.runAsync(sppTrainingTask, executorService);
//            futures.put(exchangeCode, future);
//        });
        
       waitForCompletion(futures);
    }
    
    private void waitForCompletion(Map<String, CompletableFuture<?>> futures)
    {
        CompletableFuture<?> futureFinal = CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[] {}));
        while(!futureFinal.isDone())
        {
            try
            {
                Thread.sleep(1000);
            }
            catch (InterruptedException e)
            {
                
            }
        }
    }

    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException
    {
        this.applicationContext = applicationContext;
    }
    
    private SppTrainingTask getSppTrainingTask(String exchangeCode)
    {
        return applicationContext.getBean(SppTrainingTask.class, exchangeCode);
    }
}
