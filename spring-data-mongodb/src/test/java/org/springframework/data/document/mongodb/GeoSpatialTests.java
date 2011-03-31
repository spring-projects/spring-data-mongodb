package org.springframework.data.document.mongodb;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:geospatial.xml")
public class GeoSpatialTests {

  
  @Autowired
  MongoTemplate template;

  @Before
  public void setUp() {
    template.dropCollection(template.getDefaultCollectionName());
  }
  
  @Test
  public void geoIndex() {
    assertThat(template, notNullValue());    
  }
}
