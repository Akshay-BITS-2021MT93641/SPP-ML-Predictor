package io.spp.ml.predictor.util;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class SpringContextHolder implements ApplicationContextAware
{
    private static ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException
    {
        SpringContextHolder.applicationContext = applicationContext;
    }

    public static <T> T getBean(String beanName, Class<T> beanType)
    {

        try
        {
            return applicationContext.getBean(beanName, beanType);

        }
        catch (NoSuchBeanDefinitionException noBean)
        {

            return null;
        }
    }

    public static Object getBean(String beanName)
    {

        try
        {
            return applicationContext.getBean(beanName);

        }
        catch (NoSuchBeanDefinitionException noBean)
        {

            return null;
        }
    }

    public static <T> T getBean(Class<T> beanType)
    {

        try
        {
            return applicationContext.getBean(beanType);

        }
        catch (NoSuchBeanDefinitionException noBean)
        {

            return null;
        }
    }

    public static Object getBean(String beanName, Object... args)
    {

        try
        {
            return applicationContext.getBean(beanName, args);

        }
        catch (NoSuchBeanDefinitionException noBean)
        {

            return null;
        }
    }

    public static <T> T getBean(Class<T> beanType, Object... args)
    {

        try
        {
            return applicationContext.getBean(beanType, args);

        }
        catch (NoSuchBeanDefinitionException noBean)
        {

            return null;
        }
    }

    public static <T> T getProperty(String propertyName, Class<T> propertyType)
    {

        return applicationContext.getEnvironment().getProperty(propertyName, propertyType);
    }

    public static <T> T getProperty(String propertyName, Class<T> propertyType, T defaultValue)
    {

        return applicationContext.getEnvironment().getProperty(propertyName, propertyType, defaultValue);
    }

}
