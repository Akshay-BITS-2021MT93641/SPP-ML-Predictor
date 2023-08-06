package io.spp.ml.predictor.util;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SppUtil
{
    public static Map<String, Object> getEnvironmentAsMap(Environment environment)
    {
        Map<String, Object> propertiesMap = new LinkedHashMap<>();
        
        ConfigurableEnvironment configurableEnvironment = (ConfigurableEnvironment) environment;
        
        configurableEnvironment
        .getPropertySources()
        .stream()
        .filter(ps->MapPropertySource.class.isAssignableFrom(ps.getClass()))
        .map(ps->((MapPropertySource)ps).getSource())
        .forEach(ps->propertiesMap.putAll(ps));
        
        return propertiesMap;
    }

}
