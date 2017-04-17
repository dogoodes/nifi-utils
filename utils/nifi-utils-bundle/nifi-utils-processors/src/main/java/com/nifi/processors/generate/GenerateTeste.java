//package com.nifi.processors.generate;
//
//import com.fasterxml.classmate.TypeResolver;
//import org.joda.time.LocalDate;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.SpringApplication;
//import org.springframework.boot.autoconfigure.SpringBootApplication;
//import org.springframework.context.ApplicationContext;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.ComponentScan;
//import org.springframework.http.ResponseEntity;
//import org.springframework.web.bind.annotation.RequestMethod;
//import org.springframework.web.context.request.async.DeferredResult;
//import springfox.documentation.builders.PathSelectors;
//import springfox.documentation.builders.RequestHandlerSelectors;
//import springfox.documentation.builders.ResponseMessageBuilder;
//import springfox.documentation.builders.ParameterBuilder;
//import springfox.documentation.schema.ModelRef;
//import springfox.documentation.schema.WildcardType;
//import springfox.documentation.service.ApiKey;
//import springfox.documentation.service.AuthorizationScope;
//import springfox.documentation.service.SecurityReference;
//import springfox.documentation.service.Tag;
//import springfox.documentation.spi.DocumentationType;
//import springfox.documentation.spi.service.contexts.SecurityContext;
//import springfox.documentation.spring.web.plugins.Docket;
//import springfox.documentation.swagger.web.ApiKeyVehicle;
//import springfox.documentation.swagger.web.SecurityConfiguration;
//import springfox.documentation.swagger.web.UiConfiguration;
//import springfox.documentation.swagger2.annotations.EnableSwagger2;
//import springfox.petstore.controller.PetController;
//
//import java.time.LocalDate;
//import java.util.List;
//
//public class GenerateTeste {
//
//	public static void main(String[] args) {
//
//		TypeResolver typeResolver = new TypeResolver();
//
//
//		new Docket(DocumentationType.SWAGGER_2)
//				.select()
//				.apis(RequestHandlerSelectors.any())
//				.paths(PathSelectors.any())
//				.build()
//				.pathMapping("/")
//				.directModelSubstitute(LocalDate.class, String.class)
//				.genericModelSubstitutes(ResponseEntity.class)
//				.alternateTypeRules(
//						newRule(typeResolver.resolve(DeferredResult.class,
//								typeResolver.resolve(ResponseEntity.class, WildcardType.class)),
//								typeResolver.resolve(WildcardType.class)))
//				.useDefaultResponseMessages(false)
//				.globalResponseMessage(RequestMethod.GET,
//						newArrayList(new ResponseMessageBuilder()
//								.code(500)
//								.message("500 message")
//								.responseModel(new ModelRef("Error"))
//								.build()))
//				.securitySchemes(newArrayList(apiKey()))
//				.securityContexts(newArrayList(securityContext()))
//				.enableUrlTemplating(true)
//				.globalOperationParameters(
//						newArrayList(new ParameterBuilder()
//								.name("someGlobalParameter")
//								.description("Description of someGlobalParameter")
//								.modelRef(new ModelRef("string"))
//								.parameterType("query")
//								.required(true)
//								.build()))
//				.tags(new Tag("Pet Service", "All apis relating to pets"))
//				.additionalModels(typeResolver.resolve(AdditionalModel.class))
//				;
//
//	}
//
//	private SecurityContext securityContext() {
//		return SecurityContext.builder()
//				.securityReferences(defaultAuth())
//				.forPaths(PathSelectors.regex("/anyPath.*"))
//				.build();
//	}
//
//	private ApiKey apiKey() {
//		return new ApiKey("mykey", "api_key", "header");
//	}
//
//	private SecurityContext securityContext() {
//		return SecurityContext.builder()
//				.securityReferences(defaultAuth())
//				.forPaths(PathSelectors.regex("/anyPath.*"))
//				.build();
//	}
//
//	List<SecurityReference> defaultAuth() {
//		AuthorizationScope authorizationScope
//				= new AuthorizationScope("global", "accessEverything");
//		AuthorizationScope[] authorizationScopes = new AuthorizationScope[1];
//		authorizationScopes[0] = authorizationScope;
//		return newArrayList(
//				new SecurityReference("mykey", authorizationScopes));
//	}
//
//	@Bean
//	SecurityConfiguration security() {
//		return new SecurityConfiguration(
//				"test-app-client-id",
//				"test-app-client-secret",
//				"test-app-realm",
//				"test-app",
//				"apiKey",
//				ApiKeyVehicle.HEADER,
//				"api_key",
//				"," /*scope separator*/);
//	}
//
//	@Bean
//	UiConfiguration uiConfig() {
//		return new UiConfiguration(
//				"validatorUrl",// url
//				"none",       // docExpansion          => none | list
//				"alpha",      // apiSorter             => alpha
//				"schema",     // defaultModelRendering => schema
//				UiConfiguration.Constants.DEFAULT_SUBMIT_METHODS,
//				false,        // enableJsonEditor      => true | false
//				true,         // showRequestHeaders    => true | false
//				60000L);      // requestTimeout => in milliseconds, defaults to null (uses jquery xh timeout)
//	}
//
//
//}