package org.springframework.persistence.document.test;

import javax.persistence.Entity;
import javax.persistence.Id;

import org.springframework.persistence.RelatedEntity;

@Entity
public class Person {

  @Id
  Long id;

  private String name;

  private int age;

  private java.util.Date birthDate;

  //	@Document // need to decide what the annotation here should be
  @RelatedEntity
  public Resume resume;

  public Person() {
  }

  public Person(String name, int age) {
    this.name = name;
    this.age = age;
    this.birthDate = new java.util.Date();
  }

  public void birthday() {
    ++age;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public int getAge() {
    return age;
  }

  public void setAge(int age) {
    this.age = age;
  }

  public java.util.Date getBirthDate() {
    return birthDate;
  }

  public void setBirthDate(java.util.Date birthDate) {
    this.birthDate = birthDate;
  }

  public Resume getResume() {
    return resume;
  }

  public void setResume(Resume resume) {
    this.resume = resume;
  }

}
