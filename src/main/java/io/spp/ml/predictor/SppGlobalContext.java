package io.spp.ml.predictor;

import io.fop.context.ApplicationContextKey;
import io.fop.context.ParameterizedTypeReference;
import io.fop.context.impl.ConcurrentApplicationContextImpl;

public class SppGlobalContext extends ConcurrentApplicationContextImpl
{
    public SppGlobalContext()
    {
        super(SppGlobalContext.class.getSimpleName());
    }
    
    public static final ApplicationContextKey<String> trainingStartDateKey = ApplicationContextKey.of("startDate", ParameterizedTypeReference.forType(String.class));
    public static final ApplicationContextKey<String> trainingEndDateKey = ApplicationContextKey.of("endDate", ParameterizedTypeReference.forType(String.class));
    public static final ApplicationContextKey<String> exchangeKey = ApplicationContextKey.of("exchange", ParameterizedTypeReference.forType(String.class));
    public static final ApplicationContextKey<String> indexKey = ApplicationContextKey.of("index", ParameterizedTypeReference.forType(String.class));
    public static final ApplicationContextKey<String> exchangeCodeKey = ApplicationContextKey.of("exchangeCode", ParameterizedTypeReference.forType(String.class));
}
