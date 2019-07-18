package pers.xiaoming.kafka.basic_kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyUtils {
    public static Properties loadProperties(String propertyFileName) throws IOException {
        Properties properties = new Properties();
        try (final InputStream inputStream = PropertyUtils.class.getClassLoader().getResourceAsStream(propertyFileName)){
            if (inputStream != null) {
                properties.load(inputStream);
            }
        }
        return properties;
    }
}
