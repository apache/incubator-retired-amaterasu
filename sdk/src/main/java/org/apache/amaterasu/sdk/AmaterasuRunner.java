package org.apache.amaterasu.sdk;

import java.util.Map;

/**
  * Created by eyalbenivri on 07/09/2016.
  */
public interface AmaterasuRunner {

  String getIdentifier();
  void executeSource(String actionSource, String actionName, Map<String, String> exports);

}