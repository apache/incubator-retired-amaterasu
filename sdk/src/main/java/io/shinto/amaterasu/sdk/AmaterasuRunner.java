package io.shinto.amaterasu.sdk;

/**
  * Created by eyalbenivri on 07/09/2016.
  */
public interface AmaterasuRunner {

  String getIdentifier();
  void executeSource(String actionSource, String actionName);

}