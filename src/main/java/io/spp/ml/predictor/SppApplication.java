package io.spp.ml.predictor;

import static io.spp.ml.predictor.SppGlobalContext.exchangeCodeKey;
import static io.spp.ml.predictor.SppGlobalContext.exchangeKey;
import static io.spp.ml.predictor.SppGlobalContext.indexKey;
import static io.spp.ml.predictor.SppGlobalContext.trainingEndDateKey;
import static io.spp.ml.predictor.SppGlobalContext.trainingStartDateKey;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

import io.fop.context.impl.ApplicationContextHolder;
import io.spp.ml.predictor.dao.QueryFiles;
import io.spp.ml.predictor.dao.SppMLTrainingDao;
import io.spp.ml.predictor.training.SppTrainer;
import io.spp.ml.predictor.util.SppUtil;
import io.spp.ml.predictor.util.SpringContextHolder;

@Configuration
@PropertySource({"classpath:application-${env:local}.properties"})
public class SppApplication
{    
    public static void main(String[] args)
    {
        QueryFiles.load();
        String trainingStartDate = args[0];
        String trainingEndDate = args[1];
        String exchange = args[2];
        String index = args[3];
        String exchangeCode = args.length==5 ? args[4] : null;
        
        SppGlobalContext sppGlobalContext = new SppGlobalContext();
        sppGlobalContext.add(trainingStartDateKey, trainingStartDate);
        sppGlobalContext.add(trainingEndDateKey, trainingEndDate);
        sppGlobalContext.add(exchangeKey, exchange);
        sppGlobalContext.add(indexKey, index);
        sppGlobalContext.add(exchangeCodeKey, exchangeCode);
        ApplicationContextHolder.setGlobalContext(sppGlobalContext);
        
        try(ConfigurableApplicationContext sppAppContext = new AnnotationConfigApplicationContext(SppApplication.class))
        {
            SppTrainer sppTrainer = sppAppContext.getBean(SppTrainer.class);
            sppTrainer.train();
        }
    }
    
    @Bean
    public SparkSession sparkSession(Environment environment)
    {
        return SparkSession.builder()
                                        .appName("SPP")
                                        .config(SppUtil.getEnvironmentAsMap(environment))
                                        .getOrCreate();
    }
    
    @Bean
    public SppTrainer sppTrainer()
    {
        return new SppTrainer();
    }
    
    @Bean
    public SppMLTrainingDao sppMLTrainingDao()
    {
        return new SppMLTrainingDao();
    }
    
    @Bean
    public SpringContextHolder springContextHolder()
    {
        return new SpringContextHolder();
    }
}
