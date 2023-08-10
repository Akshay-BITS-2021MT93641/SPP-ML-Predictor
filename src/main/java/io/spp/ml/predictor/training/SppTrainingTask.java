//package io.spp.ml.predictor.training;
//
//import javax.annotation.Nullable;
//
//import org.apache.spark.ml.feature.VectorAssembler;
//import org.apache.spark.ml.linalg.Vectors;
//import org.apache.spark.ml.regression.LinearRegression;
//import org.apache.spark.ml.regression.LinearRegressionModel;
//import org.apache.spark.ml.regression.LinearRegressionSummary;
//import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.springframework.beans.factory.annotation.Autowired;
//
//import io.spp.ml.predictor.dao.SppMLTrainingDao;
//import lombok.RequiredArgsConstructor;
//
//@RequiredArgsConstructor
//public class SppTrainingTask implements Runnable
//{
//    @Nullable
//    private final String exchangeCode;
//    
//    @Autowired
//    private SppMLTrainingDao sppMLTrainingDao;
//
//    @Override
//    public void run()
//    {
//        Dataset<Row> indexReturnDf = sppMLTrainingDao.loadIndexReturns();
//        Dataset<Row> securityReturnDf = sppMLTrainingDao.loadSecurityReturns(exchangeCode);
//        Dataset<Row> securityAnalyticsDf = sppMLTrainingDao.loadSecurityAnalytics(exchangeCode);
//        Dataset<Row> securityTrainingPScoreDf = sppMLTrainingDao.loadSecurityTrainingPScore(exchangeCode);
//
//        
//        Dataset<Row> indexReturnDfForTraining = indexReturnDf.select("exchange", "index", "date", "returns.90D.return").withColumnRenamed("return", "indexReturn");
//        Dataset<Row> securityReturnDfForTraining = securityReturnDf.select("exchange", "exchangeCode", "isin", "date", "returns.90D.return").withColumnRenamed("return", "securityReturn");
//        Dataset<Row> securityAnalyticsDfForTraining = securityAnalyticsDf.select("exchange", "exchangeCode", "isin", "date",
//                                                                                                              "analytics.obv","analytics.ema","analytics.ad","analytics.wma","analytics.stoch.SlowK","analytics.stoch.SlowD","analytics.trima","analytics.rsi"
//                                                                                                              ,"analytics.tema","analytics.mama.MAMA","analytics.mama.FAMA","analytics.bbands.Real Upper Band","analytics.bbands.Real Middle Band"
//                                                                                                              ,"analytics.bbands.Real Lower Band","analytics.adx","analytics.sma","analytics.aroon.Aroon Down","analytics.aroon.Aroon Up","analytics.kama"
//                                                                                                              ,"analytics.cci","analytics.macdext.MACD","analytics.macdext.MACD_Signal","analytics.macdext.MACD_Hist","analytics.t3");
//        
//        Dataset<Row> securityTrainingPScoreDfForTraining =  securityTrainingPScoreDf.select("exchange", "exchangeCode", "isin", "date", "trainingPScore.90D.pScore");
//        
//        Dataset<Row> trainingData = securityReturnDfForTraining
//                                                    .join(securityAnalyticsDfForTraining, new String[] {"exchange", "exchangeCode", "isin", "date"})
//                                                    .join(indexReturnDfForTraining, new String[] {"exchange", "date"})
//                                                    .join(securityTrainingPScoreDfForTraining, new String[] {"exchange", "exchangeCode", "isin", "date"})
//                                                    .orderBy("date")                                                   
//                                                    .select("pScore", "indexReturn", "securityReturn",
//                                                            "obv","ema","ad","wma","SlowK","SlowD","trima","rsi"
//                                                            ,"tema","MAMA","FAMA","Real Upper Band","Real Middle Band"
//                                                            ,"Real Lower Band","adx","sma","Aroon Down","Aroon Up","kama"
//                                                            ,"cci","MACD","MACD_Signal","MACD_Hist","t3");
//        
//        VectorAssembler trainingDataAssembler = new VectorAssembler()
//                                                                        .setInputCols(new String[]{"indexReturn", "securityReturn",
//                                                                                "obv","ema","ad","wma","SlowK","SlowD","trima","rsi"
//                                                                                ,"tema","MAMA","FAMA","Real Upper Band","Real Middle Band"
//                                                                                ,"Real Lower Band","adx","sma","Aroon Down","Aroon Up","kama"
//                                                                                ,"cci","MACD","MACD_Signal","MACD_Hist","t3"})
//                                                                        .setOutputCol("features");
//        
//        Dataset<Row> trainingDataDf = trainingDataAssembler.transform(trainingData).select("pScore", "features").withColumnRenamed("pScore", "label");
//        
//        Dataset<Row> trainingSetDf = trainingDataDf.limit((int)(trainingDataDf.count()/2));
//        Dataset<Row> validationSetDf = trainingDataDf.except(trainingSetDf);
//        
//        LinearRegression lr = new LinearRegression().setMaxIter(10000).setRegParam(0.3).setElasticNetParam(0.8);
//        
//        // Fit the model.
//        LinearRegressionModel lrModel = lr.fit(trainingSetDf);
//        
//        System.out.println("Coefficients: " + lrModel.coefficients() + " Intercept: " + lrModel.intercept());
//        
//        // Summarize the model over the training set and print out some metrics.
//        LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
//        System.out.println("numIterations: " + trainingSummary.totalIterations());
//        System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
//        trainingSummary.residuals().show();
//        System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
//        System.out.println("r2: " + trainingSummary.r2());
//        
//        LinearRegressionSummary validationSummary = lrModel.evaluate(validationSetDf);
//        System.out.println("meanAbsoluteError: " + validationSummary.meanAbsoluteError());
//        validationSummary.predictions().show(false);
//        validationSummary.residuals().show(false);
//        System.out.println("RMSE: " + validationSummary.rootMeanSquaredError());
//        System.out.println("r2: " + validationSummary.r2());
//        
//    }
//    
////    private SppMLTrainingDao getSppMLTrainingDao()
////    {
////        return SpringContextHolder.getBean(SppMLTrainingDao.class);
////    }
//
//}
