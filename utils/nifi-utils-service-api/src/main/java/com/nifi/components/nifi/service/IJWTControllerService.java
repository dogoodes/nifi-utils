package com.nifi.components.nifi.service;

import java.util.Map;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;

/**
 * Definition for Configuration Service.
 *
 */
@Tags({ "jwt", "security", "rest" })
@CapabilityDescription("")
public interface IJWTControllerService extends ControllerService {
	
	public Map<String, Object> getPropertyClains();
	
	public String getSignateJwt();
    
    public String getAlgorithmJwt();
    
    public Integer getExpirationTimeJwt();
    
    public String getReturnValueJwt();

}
