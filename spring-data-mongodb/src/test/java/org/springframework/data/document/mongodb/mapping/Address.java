/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.document.mongodb.mapping;

import java.io.Serializable;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class Address implements Serializable {

  private String[] lines;
  private String city;
  private String provinceOrState;
  private Integer postalCode;
  private String country;

  public String[] getLines() {
    return lines;
  }

  public void setLines(String[] lines) {
    this.lines = lines;
  }

  public String getCity() {
    return city;
  }

  public void setCity(String city) {
    this.city = city;
  }

  public String getProvinceOrState() {
    return provinceOrState;
  }

  public void setProvinceOrState(String provinceOrState) {
    this.provinceOrState = provinceOrState;
  }

  public Integer getPostalCode() {
    return postalCode;
  }

  public void setPostalCode(Integer postalCode) {
    this.postalCode = postalCode;
  }

  public String getCountry() {
    return country;
  }

  public void setCountry(String country) {
    this.country = country;
  }
}
