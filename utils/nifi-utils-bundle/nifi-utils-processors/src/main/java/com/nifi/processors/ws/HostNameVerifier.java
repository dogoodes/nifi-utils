package com.nifi.processors.ws;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;

public class HostNameVerifier implements HostnameVerifier{

	private final String subjectName;
	
	public HostNameVerifier(String subjectName){
		this.subjectName = subjectName;
	}
	
	@Override
	public boolean verify(String hostname, SSLSession session) {
		return hostname.equals(subjectName);
	}

}
