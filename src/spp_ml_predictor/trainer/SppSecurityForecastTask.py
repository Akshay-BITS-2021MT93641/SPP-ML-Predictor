
from datetime import datetime, timezone

import pandas as pd
import time
from ..trainer import SppArima
from ..trainer import SppDecisionTree

class SppSecurityForecastTask:
    def __init__(self, forecastIndexReturns:pd.DataFrame, securityDataForExchangeCode:pd.DataFrame, ctx:dict, xtraDataPdf:pd.DataFrame):
        self.forecastIndexReturns = forecastIndexReturns
        self.securityDataForExchangeCode = securityDataForExchangeCode
        self.ctx = ctx
        self.xtraDataPdf = xtraDataPdf

    def buildModel(self) -> pd.DataFrame:

        startT = time.time();
        securityDataForExchangeCodeForTraining = self.securityDataForExchangeCode.rename(columns={"securityReturns90D": "value"})
        forecast = self.invokeRegressor(securityDataForExchangeCodeForTraining)

        forecastedSecurityReturn = forecast['value'][0]
        forecastedIndexReturn = self.forecastIndexReturns["forecast"+str(self.ctx['forecastDays'])+"DIndexReturns"][0]
        forecastedPScore = (forecastedSecurityReturn - forecastedIndexReturn) * 100

        forecast.drop("value", axis=1, inplace=True)
        forecast.insert(0, "exchange", securityDataForExchangeCodeForTraining['exchange'][0])
        forecast.insert(1, "index", self.forecastIndexReturns['index'][0])
        forecast.insert(2, "exchangeCode", securityDataForExchangeCodeForTraining['exchangeCode'][0])
        forecast.insert(3, "isin", securityDataForExchangeCodeForTraining['isin'][0])
        forecast.insert(4, "date", self.ctx['pScoreDate'])
        forecast["forecastedIndexReturn"] = [forecastedIndexReturn]
        forecast["forecastedSecurityReturn"] = [forecastedSecurityReturn]
        forecast["forecastPeriod"] = [str(self.ctx['forecastDays'])+"D"]
        forecast["forecastedPScore"] = [forecastedPScore]
        forecast["lastUpdatedTimestamp"] = [datetime.strftime(datetime.now(timezone.utc), '%Y-%m-%dT%H:%M:%S%z')]
        endT = time.time()
        print("SppSecurityForecastTask - Time taken:"+str(endT-startT)+" secs")
        return forecast

    def invokeRegressor(self, securityDataForExchangeCodeForTraining:pd.DataFrame) ->pd.DataFrame:
        regressor = self.ctx['regressor']
        match regressor:
            case "SppArima":
                sppRegressor = SppArima.SppArima(securityDataForExchangeCodeForTraining, self.ctx, self.xtraDataPdf)
                return sppRegressor.forecast()
            case "SppDecisionTree":
                sppRegressor = SppDecisionTree.SppDecisionTree(securityDataForExchangeCodeForTraining, self.ctx, self.xtraDataPdf)
                return sppRegressor.forecast()
            case default:
                return None