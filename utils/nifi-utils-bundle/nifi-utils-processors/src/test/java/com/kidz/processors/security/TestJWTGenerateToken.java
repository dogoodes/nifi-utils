package com.kidz.processors.security;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import com.nifi.processors.security.JWTGenerateToken;

public class TestJWTGenerateToken {
	
//	@Test
	public void testOnTrigger() throws InitializationException {
		final TestRunner runner = TestRunners.newTestRunner(new JWTGenerateToken());
		
		runner.setProperty("Nome do segredo para gerar o token.", "teste");
		runner.setProperty("Nome do token.", "Authorization");
		
		ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        runner.enqueue(ff);
		
		runner.run();
	}
}