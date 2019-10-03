package org.apache.nifi.processors.translation.azure.model;

public class Translation {

	/**
	 * Json Response examples
	 * 
	 * Request: 
	 * [
	 * 	{'Text':'Bonjour'},
	 *  {'Text': 'RT @fmsovnk_: Bientôt 2020 et vous mettez des crochets fermés sur l’infini https://t.co/VPkXFImvn0'}
	 * ]
	 * 
	 * Response:
	 * [
	 * {"detectedLanguage":
	 * 	{"language":"fr","score":1.0},
	 * 		"translations":
	 * [
	 * 		{"text":"Hello","to":"en"}]},
	 * 
	 * 	{"detectedLanguage":{"language":"fr","score":1.0},
	 * 		"translations":
	 * 		[{"text":"RT @fmsovnk_Soon 2020 and you put closed hooks on infinity https://t.co/VPkXFImvn0","to":"en"}
	 * ]}]
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * 
	 * [ { "detectedLanguage": { "language": "th", "score": 0.89 }, "translations":
	 * [ { "text": "RT @Kinc_PNG: The end of my life is requesting this machine
	 * https://t.co/TAnrLvvIUG", "to": "en" }, { "text": "RT @GemstoneHappy: Getting
	 * what you want may be a really scary thing\n\nI may not have tried to fulfill
	 * my wish because I knew potentially that I would feel this fear. But I want to
	 * see the scenery beyond this.\n\nI want to go over fear. I...", "to": "en" } ]
	 * } ]
	 * 
	 * 
	 * 
	 * 
	 * ]
	 */

	private String fromLang;
	private String[] toLang;
	private String[] fromText;
	private String[] toText;

	String getFromLang() {
		return fromLang;
	}

	void setFromLang(String fromLang) {
		this.fromLang = fromLang;
	}

	String[] getToLang() {
		return toLang;
	}

	void setToLang(String[] toLang) {
		this.toLang = toLang;
	}

	String[] getFromText() {
		return fromText;
	}

	void setFromText(String[] fromText) {
		this.fromText = fromText;
	}

	String[] getToText() {
		return toText;
	}

	void setToText(String[] toText) {
		this.toText = toText;
	}

}
