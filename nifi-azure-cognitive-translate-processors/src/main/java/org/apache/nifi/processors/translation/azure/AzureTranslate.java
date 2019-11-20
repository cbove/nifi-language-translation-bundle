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
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
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
//@SupportsBatching 
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"translate", "translation", "azure", "language", "azure cognitive services"})
@CapabilityDescription("Translates content and/or \"Input Text\"(input-text-attribute) attribute from any supported(https://docs.microsoft.com/en-us/azure/cognitive-services/translator/reference/v3-0-languages) language to any other supported language.  ")
@WritesAttributes({
	@WritesAttribute(attribute="acs-error-code", description="If an error occurs, the request will also return a JSON error response. The error code is a 6-digit number combining the 3-digit HTTP status code followed by a 3-digit number to further categorize the error."),
	@WritesAttribute(attribute="acs-error-message", description="Description of error condition.")}
)
public class AzureTranslate extends AbstractProcessor {
	// Omitting all optional parameters due to no requirement to include
	final private static String api_version = "3.0";
	
	private OkHttpClient client;

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;

	final private static String acs_response_code = "acs-error-code";
	final private static String acs_response_message = "acs-error-message";

	// Request Headers
	private static String protocol = "https";
	private static String subscription_region = System.getenv("ACS_TRANSLATOR_TEXT_SUBCRIPTION_REGION");
	private static String subscription_key = System.getenv("ACS_TRANSLATOR_TEXT_SUBSCRIPTION_KEY");
	private static String service_endpoint = System.getenv("ACS_TRANSLATOR_TEXT_ENDPOINT");
	private static String character_set = "UTF-8";
//	private static String endpoint = "https://api-nam.cognitive.microsofttranslator.com";
	private static String default_input_text = "Лучшее время, чтобы посадить дерево, было 20 лет назад. Следующее лучшее время – сегодня.";

	
//	Property Descriptors:
//		SUBSCRIPTION_KEY: by default is set by env var
//		SUBSCRIPTION_REGION: by default is set by env var
//		SERVICE_ENDPOINT:  by default is set by env var
//		
//		API_VERSION: basically an Azure constant
//		CHARACTER_SET: default is "UTF-8"

//		fromLanguage: optional, if not set ACS will attempt to identify the language and the response will include a confidence score between 0-1
//		toLanguage: required
//		input_text: set this attribute to the content to be translated
//		translate_content: if this is TRUE then the processor will include the content of the FlowFile in the translation request
	static final PropertyDescriptor SUBSCRIPTION_KEY = new PropertyDescriptor.Builder()
			.displayName("Subscription Key")
			.name("subscription-key")
			.description("Azure Cognitive Services Subscription Key").defaultValue(subscription_key)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.sensitive(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(true).build();
	static final PropertyDescriptor SUBSCRIPTION_REGION = new PropertyDescriptor.Builder()
			.displayName("Subscription Region")
			.name("subscription-region")
			.description("Azure Cognitive Services Subscription Region").defaultValue(subscription_region)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(true).build();
	static final PropertyDescriptor SERVICE_ENDPOINT = new PropertyDescriptor.Builder()
			.displayName("Service Endpoint")
			.name("service-endpoint")
			.description("Azure Cognitive Service Endpoint").defaultValue(service_endpoint)
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR).required(true).build();
	
	static final PropertyDescriptor API_VERSION = new PropertyDescriptor.Builder()
			.displayName("API Version")
			.name("api-version")
			.description("Version of the API requested by the client. Value must be 3.0.")
			.defaultValue(api_version)
//			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
//			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.allowableValues(api_version).required(true).build();
	
	static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
			.displayName("Character Set")
			.name("character-set")
			.description("Specifies the character set of the data to be translated")
			.required(true)
			.defaultValue(character_set).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.addValidator(StandardValidators.CHARACTER_SET_VALIDATOR).build();

	
// these .name values are used when building the Request and depend on the ACS API
//	* from
//	* to
//	* api-version(above with the constants)
	static final PropertyDescriptor FROM_LANGUAGE = new PropertyDescriptor.Builder()
			.displayName("Input Language")
			.name("from")
			.description("The language of the text to be translated.  "
					+ "The language is specified by providing a well-formed BCP 47 language tag. "
					+ "For instance, use the value `ru` to specify Russian or use the value `zh-Hant` to specify Chinese Traditional.  "
					+ "Language auto-detection will be applied if not specified.")
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.addValidator(StandardValidators.NON_BLANK_VALIDATOR).required(false).build();
	static final PropertyDescriptor TO_LANGUAGE = new PropertyDescriptor.Builder()
			.displayName("Target Language(s)")
			.name("to")
			.description("The language(s) into which the text will translated."
					+ "The language is specified by providing a well-formed BCP 47 language tag. "
					+ "For instance, use the value `ru` to specify Russian or use the value `zh-Hant` to specify Chinese Traditional.  "
					+ "For multiple translations separate output languages separated by commas.")
			.defaultValue("en").expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(true).build();
	static final PropertyDescriptor INPUT_TEXT = new PropertyDescriptor.Builder()
			.displayName("Input Text")
			.name("input-text-attribute")
			.description("The text to be translated.")
			.defaultValue(default_input_text)//this is just for development
			.expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
			.addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
			.required(false)
//			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(true).
			.build();

//	Seems confusing to have both, should FlowFile content be a separate Processor to avoid confusion?
//	Could inputText Property be disabled when translateContent is set to TRUE?
//	This requires the FlowFile content to be purely the object of the translation
//	public static final PropertyDescriptor TRANSLATE_CONTENT = new PropertyDescriptor.Builder()
//			.displayName("Translate Content")
//			.name("translate-content")
//			.description(
//					"Specifies whether or not the FlowFile content should be translated. If false, only the text specified by \"Input Text\" property will be translated.")
//			.required(true).allowableValues("true", "false")
//			.defaultValue("false").build();
	
	
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}
	public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return this.descriptors;
	}

	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success")
			.description("This relationship is used when the translation is successful").build();
	public static final Relationship REL_COMMS_FAILURE = new Relationship.Builder()
			.name("comms_failure").description(
			"This relationship is used when the translation fails due to a problem such as a network failure, "
			+ "and for which the translation should be attempted again")
			.build();
	public static final Relationship REL_TRANSLATION_FAILED = new Relationship.Builder().
			name("translation_failure")
			.description(
					"This relationship is used if the translation cannot be performed for some reason other than communications failure.  Most likely a malformed request.")
			.build();

	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<>();
		
		descriptors.add(SUBSCRIPTION_KEY);
		descriptors.add(SUBSCRIPTION_REGION);
		descriptors.add(SERVICE_ENDPOINT);
		descriptors.add(API_VERSION);
		descriptors.add(CHARACTER_SET);
		descriptors.add(FROM_LANGUAGE);
		descriptors.add(TO_LANGUAGE);
		descriptors.add(INPUT_TEXT);
//		descriptors.add(TRANSLATE_CONTENT);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_COMMS_FAILURE);
		relationships.add(REL_TRANSLATION_FAILED);
		this.relationships = Collections.unmodifiableSet(relationships);
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
		String from_language = context.getProperty(FROM_LANGUAGE).evaluateAttributeExpressions(flowFile).getValue();
		String to_language = context.getProperty(TO_LANGUAGE).evaluateAttributeExpressions(flowFile).getValue();

		String input_text = context.getProperty(INPUT_TEXT).evaluateAttributeExpressions(flowFile).getValue();
		String encoding = context.getProperty(CHARACTER_SET).evaluateAttributeExpressions(flowFile).getValue();
//		boolean translate_content = context.getProperty(TRANSLATE_CONTENT).evaluateAttributeExpressions(flowFile).asBoolean().booleanValue();
		
		
		JsonArray translationTextList = new JsonArray();

//		There may be 2 inputs:
//			* input_text Attribute AND/OR
//			* flowFile content and/or input_text Attribute.		
		if(null != input_text && "" != input_text) {
			JsonObject translationText = new JsonObject();
			translationText.addProperty("Text", input_text);
			translationTextList.add(translationText);
		}
		
//		if(translate_content) {
//			byte[] buff = new byte[(int) flowFile.getSize()];
//			session.read(flowFile, new InputStreamCallback() {
//				@Override
//				public void process(final InputStream in) throws IOException {
//					StreamUtils.fillBuffer(in, buff);
//				}
//			});
//			JsonObject translationText = new JsonObject();
//			String content = new String(buff, Charset.forName(encoding));
//			translationText.addProperty("Text", content);
//			translationTextList.add(translationText);
//		}
		String json = translationTextList.toString();
		
		this.getLogger().debug("\n^^^^^^^^^^^^^^^^^^^^: " + json + " :^^^^^^^^^^^^^^^^^^^^");
		RequestBody body = RequestBody.create(json, MediaType.get("application/json"));		
		
		
		HttpUrl.Builder builder = new HttpUrl.Builder();
		builder
			.scheme(protocol)
			.host(service_endpoint)
			.addPathSegment("/translate")
			.addQueryParameter(API_VERSION.getName(), api_version)
			.addQueryParameter(TO_LANGUAGE.getName(), to_language);
		if (null != from_language)
			builder.addQueryParameter(FROM_LANGUAGE.getName(), from_language);
		
		HttpUrl httpUrl = builder.build();

		Request request = new Request.Builder()
				.url(httpUrl)
				.post(body)
				.addHeader("Ocp-Apim-Subscription-Key", sub_key)
				.addHeader("Ocp-Apim-Subscription-Region", sub_region)
				.addHeader("Content-Type", "application/json").build();
		this.getLogger().debug("\n=========================================>: " + request.toString());

		final Response response;
		Map<String, String> errorMap = new HashMap<String, String>();

		try {
			response = client.newCall(request).execute();

			String responseBody = prettify(response.body().string());
//			String responseBody = response.body().string();
			this.getLogger()
				.debug("RESPONSE_BODY: " + responseBody + "\nSTATUS_CODE: " + response.code());

			int status_code = response.code();
			if(status_code == 200) {// Happy path
				flowFile = session.write(flowFile, new OutputStreamCallback() {
					public void process(final OutputStream out) throws IOException {
						out.write(responseBody.getBytes());
					}
				});
				session.transfer(flowFile, REL_SUCCESS);
				return;
			}
			else {// Bad request
				errorMap.put("acs-http-response-code", String.valueOf(status_code));

				JsonParser responseParser = new JsonParser();
				JsonElement jsonElement = responseParser.parse(responseBody);
				JsonObject jsonObject = jsonElement.getAsJsonObject();

				try {
					JsonObject error = jsonObject.get("error").getAsJsonObject();
					String code = error.get("code").getAsString();
					String message = error.get("message").getAsString();
					errorMap.put(acs_response_code, code);
					errorMap.put(acs_response_message, message);
					this.getLogger().error(acs_response_code + ": " + acs_response_message);					
				}
				catch(NullPointerException npe) {
					errorMap.put("Null Pointer Exception", npe.getMessage());
					this.getLogger().error(npe.getMessage());
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
			errorMap.put("IO Exception", e.getMessage());
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
