package org.springframework.data.document.mongodb.mapping;

import org.springframework.context.annotation.Bean;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.document.mongodb.config.AbstractMongoConfiguration;

import com.mongodb.Mongo;

public class GeoIndexedAppConfig extends AbstractMongoConfiguration {

  public static String GEO_DB = "geodb";
  public static String GEO_COLLECTION = "geolocation";
  @Bean
  public Mongo mongo() throws Exception {
    return new Mongo("localhost");
  }
  
  @Bean
  public MongoTemplate mongoTemplate() throws Exception {
    return new MongoTemplate(mongo(), "geodb", "geolocation", mappingMongoConverter());
  }
      

  public String getMappingBasePackage() {
    return "org.springframework.data.document.mongodb.mapping";
  }

}
