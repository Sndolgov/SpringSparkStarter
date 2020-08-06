package com.starter.udfRegistration;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

/**
 * @author Evgeny Borisov
 */
@Component
public class UdfRegistrarContextListener implements BeanPostProcessor {
    @Autowired
    private ApplicationContext context;

    @Autowired
    private SparkSession sparkSession;

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        RegisterUdf annotation = bean.getClass().getAnnotation(RegisterUdf.class);

        if (annotation != null) {
            String value = annotation.value();
            String udfName = value.equals("") ? bean.getClass().getName() : value;
            sparkSession.sqlContext().udf().register(udfName, (UDF1) bean, DataTypes.IntegerType);
        }

        return bean;
    }

    /* @EventListener(ContextRefreshedEvent.class)
    public void registerAllUdf() {
        Collection<Object> beans = context.getBeansWithAnnotation(RegisterUdf.class).values();
        for (Object bean : beans) {
            String value = bean.getClass()
                    .getAnnotation(RegisterUdf.class).value();
            String udfName = value.equals("") ? bean.getClass().getName() : value;
            sparkSession.sqlContext().udf().register(udfName, (UDF1) bean, DataTypes.IntegerType);
        }
    }*/
}
