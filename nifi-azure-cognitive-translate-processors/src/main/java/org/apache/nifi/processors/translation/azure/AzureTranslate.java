package org.apache.nifi.processors.translation.azure;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

//@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_ALLOWED)
public class AzureTranslate extends AbstractProcessor {
	// Skipping all optional parameters for now
	// Request Parameters
	final private static String api_version = "3.0";
	
	private OkHttpClient client;

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;


	// Request Headers
//	private static String subscription_key = "****************";
	private static String subscription_region = System.getenv("ACS_TRANSLATOR_TEXT_SUBCRIPTION_REGION");
	private static String subscription_key = System.getenv("ACS_TRANSLATOR_TEXT_SUBSCRIPTION_KEY");
	private static String service_endpoint = System.getenv("ACS_TRANSLATOR_TEXT_ENDPOINT");
	private static String character_set = "UTF-8";
//	private static String endpoint = "https://api-nam.cognitive.microsofttranslator.com";
	private static String default_input_text = "Лучшее время, чтобы посадить дерево, было 20 лет назад. Следующее лучшее время – сегодня.";
//	private static String default_input_text = "Привет, как ты сегодня?";
//	private String authorization_token;

	static final PropertyDescriptor SUBSCRIPTION_KEY = new PropertyDescriptor.Builder().name("Subscription Key")
			.description("Azure Cognitive Services Subscription Key").defaultValue(subscription_key)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.sensitive(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(true).build();
	static final PropertyDescriptor SUBSCRIPTION_REGION = new PropertyDescriptor.Builder().name("Subscription Region")
			.description("Azure Cognitive Services Subscription Region").defaultValue(subscription_region)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(true).build();

	static final PropertyDescriptor API_VERSION = new PropertyDescriptor.Builder().displayName("API Version")
			.name("api-version").description("Version of the API requested by the client. Value must be 3.0.")
			.defaultValue(api_version).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).allowableValues(api_version).required(true).build();
	static final PropertyDescriptor SERVICE_ENDPOINT = new PropertyDescriptor.Builder().name("Service Endpoint")
			.description("Azure Cognitive Service Endpoint").defaultValue(service_endpoint)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.addValidator(StandardValidators.URL_VALIDATOR).required(true).build();
	
	static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder().displayName("Character Set")
			.name("character_set")
			.description("Specifies the character set of the data to be translated")
			.required(true)
			.defaultValue(character_set).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.addValidator(StandardValidators.CHARACTER_SET_VALIDATOR).build();

	static final PropertyDescriptor fromLanguage = new PropertyDescriptor.Builder().displayName("Input Language")
			.name("from")
			.description("The language of the text to be translated.  "
					+ "The language is specified by providing a well-formed BCP 47 language tag. "
					+ "For instance, use the value `ru` to specify Russian or use the value `zh-Hant` to specify Chinese Traditional.  "
					+ "Language auto-detection will be applied if not specified.")
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR).required(false).build();
	static final PropertyDescriptor toLanguage = new PropertyDescriptor.Builder().displayName("Output Language(s)")
			.name("to")
			.description("The language(s) into which the text will translated."
					+ "The language is specified by providing a well-formed BCP 47 language tag. "
					+ "For instance, use the value `ru` to specify Russian or use the value `zh-Hant` to specify Chinese Traditional.  "
					+ "For multiple translations separate output languages separated by commas.")
			.defaultValue("en").expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(true).build();
	static final PropertyDescriptor inputText = new PropertyDescriptor.Builder().displayName("Input Text")
			.name("inputText").description("The text to be translated.").defaultValue(default_input_text)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
//			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(true).
			.build();

	
	public static final PropertyDescriptor translateContent = new PropertyDescriptor.Builder()
			.name("translateContent")
			.displayName("Translate Content")
			.description(
					"Specifies whether or not the FlowFile content should be translated. If false, only the text specified by \"Input Text\" property will be translated.")
			.required(true).allowableValues("true", "false")
			.defaultValue("false").build();

	
	
	
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}
	public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return this.descriptors;
	}

	public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
			.description("This relationship is used when the translation is successful").build();
	public static final Relationship REL_COMMS_FAILURE = new Relationship.Builder().name("comms.failure").description(
			"This relationship is used when the translation fails due to a problem such as a network failure, "
			+ "and for which the translation should be attempted again")
			.build();
	public static final Relationship REL_TRANSLATION_FAILED = new Relationship.Builder().name("translation.failure")
			.description(
					"This relationship is used if the translation cannot be performed for some reason other than communications failure.  Most likely a malformed request.")
			.build();

	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<>();
		descriptors.add(SUBSCRIPTION_KEY);
		descriptors.add(SUBSCRIPTION_REGION);
		descriptors.add(API_VERSION);
		descriptors.add(SERVICE_ENDPOINT);
		descriptors.add(CHARACTER_SET);
		descriptors.add(fromLanguage);
		descriptors.add(toLanguage);
		descriptors.add(inputText);
		descriptors.add(translateContent);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_COMMS_FAILURE);
		relationships.add(REL_TRANSLATION_FAILED);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	
	private RequestBody buildRequest(ProcessContext context, ProcessSession session) {
		FlowFile flowFile = session.get();

		String input_text = context.getProperty(inputText).evaluateAttributeExpressions(flowFile).getValue();
		String encoding = context.getProperty(CHARACTER_SET).evaluateAttributeExpressions(flowFile).getValue();
		boolean translate_content = context.getProperty(translateContent).evaluateAttributeExpressions(flowFile).asBoolean().booleanValue();
		
		
		JsonArray translationTextList = new JsonArray();

//		There may be 2 inputs:
//			* input_text Attribute AND/OR
//			* flowFile content and/or input_text Attribute.		
		if(null != input_text && "" != input_text) {
			JsonObject translationText = new JsonObject();
			translationText.addProperty("Text", input_text);
			translationTextList.add(translationText);
		}
		
		if(translate_content) {
			byte[] buff = new byte[(int) flowFile.getSize()];
			session.read(flowFile, new InputStreamCallback() {
				@Override
				public void process(final InputStream in) throws IOException {
					StreamUtils.fillBuffer(in, buff);
				}
			});
			JsonObject translationText = new JsonObject();
			String content = new String(buff, Charset.forName(encoding));
			translationText.addProperty("Text", content);
			translationTextList.add(translationText);
		}
		String json = translationTextList.getAsString();
		return RequestBody.create(json, MediaType.get("application/json"));
		
	}
	
	

	
	
	
	@Override
	public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

		FlowFile flowFile = session.get();
		if (null == flowFile)
			flowFile = session.create();
		String sub_key = context.getProperty(SUBSCRIPTION_KEY).evaluateAttributeExpressions(flowFile).getValue();
		String sub_region = context.getProperty(SUBSCRIPTION_REGION).evaluateAttributeExpressions(flowFile).getValue();
		String api_version = context.getProperty(API_VERSION).evaluateAttributeExpressions(flowFile).getValue();
		String service_endpoint = context.getProperty(SERVICE_ENDPOINT).evaluateAttributeExpressions(flowFile).getValue();
		String from_language = context.getProperty(fromLanguage).evaluateAttributeExpressions(flowFile).getValue();
		String to_language = context.getProperty(toLanguage).evaluateAttributeExpressions(flowFile).getValue();


		
//		String input_text_json = "[{\n\t\"Text\": \"" + input_text + "\"\n}]";
		RequestBody body = buildRequest(context, session);
//		if(translate_content) {
//			byte[] buff = new byte[(int) flowFile.getSize()];
//			session.read(flowFile, new InputStreamCallback() {
//				@Override
//				public void process(final InputStream in) throws IOException {
//					StreamUtils.fillBuffer(in, buff);
//				}
//			});
//			
//			if(null != input_text) {
//				byte[] inputBytes = input_text.getBytes();
//				byte[] combinedContentInput = new byte[buff.length + inputBytes.length];
//				System.arraycopy(inputBytes,  0, combinedContentInput, 0, inputBytes.length);
//				System.arraycopy(buff, 0, combinedContentInput, inputBytes.length, buff.length);
//				
//			}
//			
//			body = RequestBody.create(buff, MediaType.get("application/json"));
//
//		}
//		else {
//			body = RequestBody.create(input_text_json, MediaType.get("application/json"));
//		}
//		Request requestX = new Request.Builder()
//				.url(endpoint)
//				.
//		Strip white space from to_language
//		to_language = to_language.replaceAll("\\s+", "");

		 
		HttpUrl.Builder builder = new HttpUrl.Builder();
		builder
			.addPathSegment(service_endpoint)
			.addPathSegment("/translate")
			.addQueryParameter(API_VERSION.getName(), api_version)
			.addQueryParameter(toLanguage.getName(), to_language);
		if (null != from_language)
			builder.addQueryParameter(fromLanguage.getName(), from_language);
		
		HttpUrl httpUrl = builder.build();
		
//				
//				
//				
//				
//				
//		StringBuffer queryString = new StringBuffer();
//
//		queryString.append("/translate?");
//		queryString.append(API_VERSION.getName() + "=" + api_version + "&");
//
////		Strip white space from to_language
//		to_language  = to_language.replaceAll("\\s+", "");
//		queryString.append(toLanguage.getName() + "=" + to_language);
//		
//		if (null != from_language)
//			queryString.append("&" + fromLanguage.getName() + "=" + from_language);

//	    Build request object from endpoint and http query string
//		Request request = new Request.Builder().url(end_point + queryString.toString())
		Request request = new Request.Builder().url(httpUrl)
				.post(body)
				.addHeader("Ocp-Apim-Subscription-Key", sub_key)
				.addHeader("Ocp-Apim-Subscription-Region", sub_region)
				.addHeader("Content-Type", "application/json").build();
		this.getLogger().debug("****: " + request.toString());
		this.getLogger().debug("****: " + body.toString());

		final Response response;
		Map<String, String> errorMap = new HashMap<String, String>();

		try {
			response = client.newCall(request).execute();

			String responseBody = prettify(response.body().string());
//			String responseBody = response.body().string();
			this.getLogger()
				.debug(responseBody);

			int status_code = response.code();
			if(status_code == 200) {// Happy path
				flowFile = session.write(flowFile, new OutputStreamCallback() {
					public void process(final OutputStream out) throws IOException {
						out.write(responseBody.getBytes());
					}
				});
				session.transfer(flowFile, REL_SUCCESS);
//				return;
			}
			else {// Bad request
				errorMap.put("http-response-code", String.valueOf(status_code));

				JsonParser responseParser = new JsonParser();
				JsonElement json = responseParser.parse(responseBody);
				JsonObject jsonObject = json.getAsJsonObject();

				try {
					JsonObject error = jsonObject.get("error").getAsJsonObject();
					String code = error.get("code").getAsString();
					String message = error.get("message").getAsString();
					errorMap.put("code", code);
					errorMap.put("message", message);
					
				}
				catch(NullPointerException npe) {
					errorMap.put("code", String.valueOf(status_code));
					errorMap.put("message", "No message included in response");
					
				}
				flowFile = session.putAllAttributes(flowFile, errorMap);
				flowFile = session.write(flowFile, new OutputStreamCallback() {
					public void process(final OutputStream out) throws IOException {
						out.write(responseBody.getBytes());
					}
				});

				
				if(status_code >= 400 && status_code < 500) {
					session.transfer(flowFile, REL_TRANSLATION_FAILED);
				}
				else {
					session.transfer(flowFile, REL_COMMS_FAILURE);
				}
//				return;
			}

		} catch (IOException e) {
			errorMap.put("IOException", e.getMessage());
			session.transfer(flowFile, REL_COMMS_FAILURE);
			this.getLogger().error(e.getMessage());
		}
	}// end onTrigger()

	private static String prettify(String json_text) {
		JsonParser parser = new JsonParser();
		JsonElement json = parser.parse(json_text);
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		return gson.toJson(json);
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {
		client = new OkHttpClient();
	}

	@OnStopped
	public void destroyClient() {
		client = null;
	}
}
