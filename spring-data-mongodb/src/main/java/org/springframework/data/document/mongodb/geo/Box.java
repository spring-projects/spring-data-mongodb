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

/**
 * Represents a geospatial box value
 * @author Mark Pollack
 *
 */
public class Box {

  
    private double xmin;
    
    private double ymin;
    
    private double xmax;
    
    private double ymax;
           
    public Box(Point lowerLeft, Point upperRight) {
      xmin = lowerLeft.getX();
      ymin = lowerLeft.getY();
      xmax = upperRight.getX();
      ymax = upperRight.getY();
    }
    
    public Box(double[] lowerLeft, double[] upperRight) {
      xmin = lowerLeft[0];
      ymin = lowerLeft[1];
      xmax = upperRight[0];
      ymax = upperRight[1];
    }
    
    public Point getLowerLeft() {
      return new Point(xmin, ymin);
    }
    
    public Point getUpperRight() {
      return new Point(xmax, ymax);
    }
    @Override
    public String toString() {
      return "Box [xmin=" + xmin + ", ymin=" + ymin + ", xmax=" + xmax
          + ", ymax=" + ymax + "]";
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      long temp;
      temp = Double.doubleToLongBits(xmax);
      result = prime * result + (int) (temp ^ (temp >>> 32));
      temp = Double.doubleToLongBits(xmin);
      result = prime * result + (int) (temp ^ (temp >>> 32));
      temp = Double.doubleToLongBits(ymax);
      result = prime * result + (int) (temp ^ (temp >>> 32));
      temp = Double.doubleToLongBits(ymin);
      result = prime * result + (int) (temp ^ (temp >>> 32));
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      Box other = (Box) obj;
      if (Double.doubleToLongBits(xmax) != Double.doubleToLongBits(other.xmax))
        return false;
      if (Double.doubleToLongBits(xmin) != Double.doubleToLongBits(other.xmin))
        return false;
      if (Double.doubleToLongBits(ymax) != Double.doubleToLongBits(other.ymax))
        return false;
      if (Double.doubleToLongBits(ymin) != Double.doubleToLongBits(other.ymin))
        return false;
      return true;
    }

}
