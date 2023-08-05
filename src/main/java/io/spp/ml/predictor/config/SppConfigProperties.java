package io.spp.ml.predictor.config;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

@Log4j2
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SppConfigProperties
{
    private static final Properties properties = new Properties();
    private static final ReadWriteLock propertiesLock = new ReentrantReadWriteLock(true);
    
    public static void load(String configFile)
    {
        propertiesWriteOperation(props->
        {
            try
            {
                props.clear();
                props.load(SppConfigProperties.class.getClassLoader().getResourceAsStream(configFile));
                return Boolean.TRUE;
            }
            catch (IOException e)
            {
                log.error("load error", e);
                return Boolean.FALSE;
            }
            
        });
    }
    
    public static <T> T getProperty(String key, Class<T> valueType, T defaultValue)
    {
        return propertiesReadOperation(props->valueType.cast(props.getOrDefault(key, defaultValue)));
    }
    
    public static <T> T getProperty(String key, Class<T> valueType)
    {
        return getProperty(key, valueType, null);
    }
    
    public static String getString(String key, String defaultValue)
    {
        return propertiesReadOperation(props->props.getProperty(key, defaultValue));
    }
    
    public static String getString(String key)
    {
        return getString(key, null);
    }
    
    public static Map<String, Object> getPropertiesMap() 
    {
        return propertiesReadOperation(props->props
                                                                    .entrySet().stream()
                                                                    .collect(Collectors.toMap(e->e.getKey().toString(), e->e.getValue())));
        
    }
    
    public static Long getLong(String key, Long defaultValue)
    {
        String valueStr = getString(key);
        return Objects.nonNull(valueStr) ? Long.valueOf(valueStr) : defaultValue;
    }
    
    public static Long getLong(String key)
    {
        return getLong(key, null);
    }
    
    public static Double getDouble(String key, Double defaultValue)
    {
        String valueStr = getString(key);
        return Objects.nonNull(valueStr) ? Double.valueOf(valueStr) : defaultValue;
    }
    
    public static Double getDouble(String key)
    {
        return getDouble(key, null);
    }
    
    private static <T> T propertiesWriteOperation(Function<Properties, T> writeOperation)
    {
        try
        {
            if(propertiesLock.writeLock().tryLock(60, TimeUnit.SECONDS)) 
            {
                try 
                {
                    return writeOperation.apply(properties);
                } 
                finally 
                {
                    propertiesLock.writeLock().unlock();
                }
                
            }
        }
        catch (InterruptedException e)
        {
            log.error("propertiesWriteOperation error.", e);
        }
        
        return null;
    }
    
    private static <T> T propertiesReadOperation(Function<Properties, T> readOperation)
    {
        try
        {
            if(propertiesLock.readLock().tryLock(60, TimeUnit.SECONDS)) 
            {
                try 
                {
                    return readOperation.apply(properties);
                } 
                finally 
                {
                    propertiesLock.readLock().unlock();
                }
                
            }
        }
        catch (InterruptedException e)
        {
            log.error("propertiesWriteOperation error.", e);
        }
        
        return null;
    }
}
