package org.apache.nifi.processors.translation.azure.model;

public class Translation {

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
