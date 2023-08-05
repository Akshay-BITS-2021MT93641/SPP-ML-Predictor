package io.spp.ml.predictor.dao;

import java.util.Arrays;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class QueryFiles
{
    public static final String SPP_STOCK_DATA_MQL = "queries/stockData.mql";
    
    public static void load()
    {
        Arrays.asList(
                SPP_STOCK_DATA_MQL
                )
        .stream()
        .forEach(qf->QueryHolder.readQueries(qf, "<", ">", "CollectionName"));
    }
}
