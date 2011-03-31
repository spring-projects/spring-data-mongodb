/*
 * Copyright 2010-2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.document.mongodb.geo;

import java.util.Arrays;

/**
 * Represents a geospatial circle value
 * @author Mark Pollack
 *
 */
public class Circle {

  private double[] center;
  private double radius;
  
  public Circle(double centerX, double centerY, double radius) {
    this.center = new double[] { centerX, centerY };
    this.radius = radius;
  }

  public double[] getCenter() {
    return center;
  }

  public double getRadius() {
    return radius;
  }

  @Override
  public String toString() {
    return "Circle [center=" + Arrays.toString(center) + ", radius=" + radius
        + "]";
  }
  
  
}
