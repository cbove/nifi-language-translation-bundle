package org.apache.nifi.processors.translation.azure;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

@TriggerWhenEmpty
public class AzureTranslate extends AbstractProcessor {
	// Skipping all optional parameters for now
	// Request Parameters
	final private static String api_version = "3.0";
	
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
	
//	private Translation translation;
	
	// Request Headers
	private static String subscription_key = "91268f3cc0554198a3ba8c4ac31967df";
	private static String subscription_region = "eastus";
	private static String endpoint = "https://api-nam.cognitive.microsofttranslator.com";
	private static String default_input_text = "Лучшее время, чтобы посадить дерево, было 20 лет назад. Следующее лучшее время – сегодня.";
//	private static String default_input_text = "Привет, как ты сегодня?";
//	private String authorization_token;

	
	static final PropertyDescriptor SUBSCRIPTION_KEY = new PropertyDescriptor.Builder()
			.name("Subscription Key")
			.description("Azure Cognitive Services Subscription Key")
			.defaultValue(subscription_key)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.required(true)
			.build();

	static final PropertyDescriptor SUBSCRIPTION_REGION = new PropertyDescriptor.Builder()
			.name("Subscription Region")
			.description("Azure Cognitive Services Subscription Region")
			.defaultValue(subscription_region)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.required(true)
			.build();	
	
	static final PropertyDescriptor API_VERSION = new PropertyDescriptor.Builder()
			.displayName("API Version")
			.name("api-version")
			.description("Version of the API requested by the client. Value must be 3.0.")
			.defaultValue(api_version)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.allowableValues(api_version)
			.required(true)
			.build();
	static final PropertyDescriptor ENDPOINT = new PropertyDescriptor.Builder()
			.name("Service Endpoint")
			.description("Azure Cognitive Service Endpoint")
			.defaultValue(endpoint)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.required(true)
			.build();
	
	static final PropertyDescriptor fromLanguage = new PropertyDescriptor.Builder()
			.displayName("Input Language")
			.name("from")
			.description("The language of the text to be translated.  " + 
					"The language is specified by providing a well-formed BCP 47 language tag. " + 
					"For instance, use the value `ru` to specify Russian or use the value `zh-Hant` to specify Chinese Traditional.  " +
					"Language auto-detection will be applied if not specified.")
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR)
			.required(false)
			.build();

	static final PropertyDescriptor toLanguage = new PropertyDescriptor.Builder()
			.displayName("Output Language(s)")
			.name("to")
			.description("The language(s) into which the text will translated." +
			"The language is specified by providing a well-formed BCP 47 language tag. " + 
			"For instance, use the value `ru` to specify Russian or use the value `zh-Hant` to specify Chinese Traditional.  " +
			"For multiple translations separate output languages separated by commas.")
			.defaultValue("en,es,it")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.required(true)
			.build();

	static final PropertyDescriptor inputText = new PropertyDescriptor.Builder()
			.displayName("Input Text")
			.name("inputText")
			.description("The text to be translated.")
			.defaultValue(default_input_text)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.required(true)
			.build();



	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<>();
		descriptors.add(SUBSCRIPTION_KEY);
		descriptors.add(SUBSCRIPTION_REGION);
		descriptors.add(API_VERSION);
		descriptors.add(ENDPOINT);
		descriptors.add(fromLanguage);
		descriptors.add(toLanguage);
		descriptors.add(inputText);
		this.descriptors = Collections.unmodifiableList(descriptors);
		
		
		final Set<Relationship> relationships = new HashSet<>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_COMMS_FAILURE);
		relationships.add(REL_TRANSLATION_FAILED);
		this.relationships = Collections.unmodifiableSet(relationships);
		
	}
	public Set<Relationship> getRelationships(){
		return this.relationships;
	}
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors(){
		return this.descriptors;
	}

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("This relationship is used when the translation is successful")
            .build();
    public static final Relationship REL_COMMS_FAILURE = new Relationship.Builder()
            .name("comms.failure")
            .description("This relationship is used when the translation fails due to a problem such as a network failure, and for which the translation should be attempted again")
            .build();
    public static final Relationship REL_TRANSLATION_FAILED = new Relationship.Builder()
            .name("translation.failure")
            .description("This relationship is used if the translation cannot be performed for some reason other than communications failure")
            .build();
	
	
	public AzureTranslate() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		
		String sub_key = context.getProperty(SUBSCRIPTION_KEY).evaluateAttributeExpressions(flowFile).getValue();
		String sub_region = context.getProperty(SUBSCRIPTION_REGION).evaluateAttributeExpressions(flowFile).getValue();
		String api_version = context.getProperty(API_VERSION).evaluateAttributeExpressions(flowFile).getValue();
		String end_point = context.getProperty(ENDPOINT).evaluateAttributeExpressions(flowFile).getValue();
		String from_language = context.getProperty(fromLanguage).evaluateAttributeExpressions(flowFile).getValue();
		String to_language = context.getProperty(toLanguage).evaluateAttributeExpressions(flowFile).getValue();
		String input_text = context.getProperty(inputText).evaluateAttributeExpressions(flowFile).getValue();
		String input_text_json = "[{\n\t\"Text\": \"" + input_text + "\"\n}]";
//		String input_text_json = "[{\n\t\"Text\": \"Welcome to Microsoft Translator. Guess how many languages I speak!\"\n}]";
		
	    OkHttpClient client = new OkHttpClient();
	    MediaType mediaType = MediaType.parse("application/json");
	    RequestBody body = RequestBody.create(mediaType, input_text_json);
	    

	    String url = endpoint + "/translate?";
	    StringBuffer queryString = new StringBuffer();
	    queryString.append(API_VERSION.getName() + "=" + api_version + "&");
	    queryString.append(toLanguage.getName() + "=" + to_language);
	    if(null != from_language)
	    	queryString.append("&" + fromLanguage.getName() + "=" + from_language);
	    	
	    
	    
	    Request request = new Request.Builder()
	    		.url(url + queryString.toString())
	    		.post(body)
	    		.addHeader("Ocp-Apim-Subscription-Key", sub_key)
	    		.addHeader("Ocp-Apim-Subscription-Region", sub_region)
	    		.addHeader("Content-Type", "application/json")
	    		.build();
//	    this.getLogger().error(prettify(request.toString()));
//	    this.getLogger().error(prettify(request.body().toString()));
	    this.getLogger().error(request.toString());
//	    this.getLogger().error("****: " + body.toString());

	    
	    Response response = null;
	    try {
			response = client.newCall(request).execute();
			
			this.getLogger().error(prettify(response.body().string()));
			
//			System.out.println(response.body().string());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    finally {
	    	response.close();
	    }
		
	}
	private static String prettify(String json_text) {
        JsonParser parser = new JsonParser();
        JsonElement json = parser.parse(json_text);
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        return gson.toJson(json);
	}
}
