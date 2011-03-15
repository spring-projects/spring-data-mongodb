package org.springframework.persistence.document.test;

import org.springframework.persistence.RelatedEntity;
import org.springframework.persistence.document.DocumentEntity;

@DocumentEntity
public class MongoPerson {

  // TODO only public because of AspectJ bug
  public String name;

  public int age;

  public java.util.Date birthDate;

  // TODO only public to check weaving bug--
  // seems to work whereas private didn't
  @RelatedEntity
  public Account account;

  public MongoPerson(String name, int age) {
    this.name = name;
    this.age = age;
    this.birthDate = new java.util.Date();
  }

  public void birthday() {
    ++age;
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

  public Account getAccount() {
    return account;
  }

  public void setAccount(Account account) {
    this.account = account;
  }

}
