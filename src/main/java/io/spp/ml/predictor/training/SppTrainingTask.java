package io.spp.ml.predictor.training;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionSummary;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import io.fop.context.impl.ApplicationContextHolder;
import io.spp.ml.predictor.SppGlobalContext;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SppTrainingTask implements Callable<Dataset<Row>>, Supplier<Dataset<Row>>
{
    private final String exchangeCode;
    
    private final Dataset<Row> securityDataAll;
    
    @Override
    public Dataset<Row> call()
    {
        return buildModel();
    }
    
    @Override
    public Dataset<Row> get()
    {
        return buildModel();
    }

    private Dataset<Row> buildModel()
    {
        Dataset<Row> securityDataForExchangeCode = securityDataAll.where(new Column("exchangeCode").equalTo(exchangeCode));
        
        Dataset<Row> trainingData = securityDataForExchangeCode.select("date", "exchange", "index", "exchangeCode", "isin", "securityReturns90D", "indexReturns90D"
                                                                                                          , "analytics.obv","analytics.ema","analytics.ad","analytics.wma","analytics.stoch.SlowK","analytics.stoch.SlowD","analytics.trima","analytics.rsi"
                                                                                                          , "analytics.tema","analytics.mama.MAMA","analytics.mama.FAMA","analytics.bbands.Real Upper Band","analytics.bbands.Real Middle Band"
                                                                                                          , "analytics.bbands.Real Lower Band","analytics.adx","analytics.sma","analytics.aroon.Aroon Down","analytics.aroon.Aroon Up","analytics.kama"
                                                                                                          , "analytics.cci","analytics.macdext.MACD","analytics.macdext.MACD_Signal","analytics.macdext.MACD_Hist","analytics.t3"
                                                                                                          , "trainingPScore90D"
                                                                                                        );
        
        SppGlobalContext sppGlobalContext = (SppGlobalContext)ApplicationContextHolder.getGlobalContext(SppGlobalContext.class.getSimpleName());
        String trainingStartDateStr = sppGlobalContext.fetch(SppGlobalContext.trainingStartDateKey);
        String trainingEndDateStr = sppGlobalContext.fetch(SppGlobalContext.trainingEndDateKey);
        LocalDate trainingStartDate = LocalDate.parse(trainingStartDateStr);
        LocalDate trainingEndDate = LocalDate.parse(trainingEndDateStr);
        long days = ChronoUnit.DAYS.between(trainingStartDate, trainingEndDate.plusDays(1));
        long trainingSetDays = (long)(days * 0.75);
        LocalDate trainingSetEndDate = trainingStartDate.plusDays(trainingSetDays);
        String trainingSetEndDateStr = trainingSetEndDate.format(DateTimeFormatter.ISO_LOCAL_DATE);
        
        
        
        VectorAssembler trainingDataAssembler = new VectorAssembler()
                                                                        .setInputCols(new String[]{"securityReturns90D", "indexReturns90D"
                                                                                , "obv","ema","ad","wma","SlowK","SlowD","trima","rsi"
                                                                                , "tema","MAMA","FAMA","Real Upper Band","Real Middle Band"
                                                                                , "Real Lower Band","adx","sma","Aroon Down","Aroon Up","kama"
                                                                                , "cci","MACD","MACD_Signal","MACD_Hist","t3"})
                                                                        .setOutputCol("features");
        
        Dataset<Row> trainingDataVectorized = trainingDataAssembler.transform(trainingData).select("date", "trainingPScore90D", "features").withColumnRenamed("trainingPScore90D", "label");
        
        Dataset<Row> trainingSetData = trainingDataVectorized.where(new Column("date").leq(trainingSetEndDateStr)).select("label", "features");
        Dataset<Row> validationSetData = trainingDataVectorized.where(new Column("date").gt(trainingSetEndDateStr)).select("label", "features");

        LinearRegression lr = new LinearRegression().setMaxIter(10000).setRegParam(0.3).setElasticNetParam(0.8);

        // Fit the model.
        LinearRegressionModel lrModel = lr.fit(trainingSetData);
        
        System.out.println("Coefficients: " + lrModel.coefficients() + " Intercept: " + lrModel.intercept());

        // Summarize the model over the training set and print out some metrics.
        LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
        System.out.println("numIterations: " + trainingSummary.totalIterations());
        System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
        trainingSummary.residuals().show();
        System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
        System.out.println("r2: " + trainingSummary.r2());

        LinearRegressionSummary validationSummary = lrModel.evaluate(validationSetData);
        System.out.println("meanAbsoluteError: " + validationSummary.meanAbsoluteError());
        validationSummary.predictions().show(false);
        validationSummary.residuals().show(false);
        System.out.println("RMSE: " + validationSummary.rootMeanSquaredError());
        System.out.println("r2: " + validationSummary.r2());
        
        return null;
    }

}
