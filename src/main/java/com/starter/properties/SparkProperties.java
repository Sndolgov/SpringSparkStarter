package com.starter.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "spark")
@Data
public class SparkProperties
{
    private String devAppName = "app";

    private String prodAppName = "app";

    private String devMaster = "local[*]";

    private String prodMaster = "local[*]";

    private int batchDuration = 1;
}
