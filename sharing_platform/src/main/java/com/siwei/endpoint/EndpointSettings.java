package com.siwei.endpoint;

import java.util.HashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties("platform.endpoints")
public class EndpointSettings {
    private String masterKey = "";
    private String keyDirectory = "";
    private Map<String, String> localRoots = new HashMap<>();
    public String getMasterKey() { return masterKey; }
    public void setMasterKey(String value) { masterKey = value; }
    public String getKeyDirectory() { return keyDirectory; }
    public void setKeyDirectory(String value) { keyDirectory = value; }
    public Map<String, String> getLocalRoots() { return localRoots; }
    public void setLocalRoots(Map<String, String> value) { localRoots = value; }
}
