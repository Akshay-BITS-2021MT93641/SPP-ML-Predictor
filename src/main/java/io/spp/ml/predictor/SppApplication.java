package io.spp.ml.predictor;

import org.apache.spark.sql.SparkSession;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

import io.spp.ml.predictor.dao.QueryFiles;
import io.spp.ml.predictor.dao.SppMLTrainingDao;
import io.spp.ml.predictor.training.SppTrainer;
import io.spp.ml.predictor.util.SppUtil;

@Configuration
@PropertySource({"classpath:application-${env:local}.properties"})
public class SppApplication
{    
    public static void main(String[] args)
    {
        QueryFiles.load();
        
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
    public SppMLTrainingDao sppMLTrainingDao()
    {
        return new SppMLTrainingDao();
    }
    
    @Bean
    public SppTrainer sppTrainer()
    {
        return new SppTrainer();
    }
}
