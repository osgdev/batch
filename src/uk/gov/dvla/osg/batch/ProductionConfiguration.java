package uk.gov.dvla.osg.batch;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ProductionConfiguration {

	private String filename, englishFleet, welshFleet, englishMulti, welshMulti, englishUnsorted, welshUnsorted,
		englishSorted, welshSorted, englishClerical, welshClerical, englishReject, welshReject, englishReprint,
		welshReprint, mailingSite, minimumMailsort, mailsortProduct;
	private static final Logger LOGGER = LogManager.getLogger(Main.class.getName());
	
	public ProductionConfiguration(String filename){
		this.filename=filename;
		LOGGER.debug("Validating file '{}'",filename);
		parseConfig(filename);
	}
	
	private void parseConfig(String filename){
		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
			String line;
		    while (  ((line = br.readLine()) != null) ) {
		    	if( !(line.startsWith("#")) ){
		    		String[] split = line.split("=");
			    	String attribute = split[0];
			    	String value = split[1];
			    	if( "site.english.fleet".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.englishFleet=value;
			    	} else if ( "site.welsh.fleet".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.welshFleet=value;
			    	} else if ( "site.english.multi".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.englishMulti=value;
			    	} else if ( "site.welsh.multi".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.welshMulti=value;
			    	} else if ( "site.english.unsorted".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.englishUnsorted=value;
			    	} else if ( "site.welsh.unsorted".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.welshUnsorted=value;
			    	} else if ( "site.english.sorted".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.englishSorted=value;
			    	} else if ( "site.welsh.sorted".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.welshSorted=value;
			    	} else if ( "site.english.clerical".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.englishClerical=value;
			    	} else if ( "site.welsh.clerical".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.welshClerical=value;
			    	} else if ( "site.english.reject".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.englishReject=value;
			    	} else if ( "site.welsh.reject".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.welshReject=value;
			    	} else if ( "site.english.reprint".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.englishReprint=value;
			    	} else if ( "site.welsh.reprint".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.welshReprint=value;
			    	} else if ( "site.mailing".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.mailingSite=value;
			    	} else if ( "minimum.mailsort".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.minimumMailsort=value;
			    	} else if ( "mailsort.preference.product".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.mailsortProduct=value;
			    	}
		    	}
		    	
		    }
		} catch (FileNotFoundException e) {
			LOGGER.fatal("Lookup file error: '{}'",e.getMessage());
			System.exit(1);
		} catch (IOException e) {
			LOGGER.fatal("Lookup file error: '{}'",e.getMessage());
			System.exit(1);
		}
	}
	
	private boolean isValid(String att, String val){
		boolean result = true;
		if( att.startsWith("site") || att.startsWith("SITE") ){
			if( isNumeric(val) ){
				int value = Integer.parseInt(val);
				if( (value > 100) || (value < 0) ){
					result = false;
				}
			}else{
				if( !("M".equalsIgnoreCase(val)) && 
						!("F".equalsIgnoreCase(val)) && 
						!("X".equalsIgnoreCase(val)) &&
						!("XX".equalsIgnoreCase(val))){
					result = false;
				}
			}
		}

		if( "minimum.mailsort".equalsIgnoreCase(att) && !(isNumeric(val)) ){
			result = false;
		}
		if( "mailsort.preference.product".equalsIgnoreCase(att) && 
				!("OCR".equalsIgnoreCase(val)) &&
				!("MM".equalsIgnoreCase(val)) &&
				!("UNSORTED".equalsIgnoreCase(val)) ){
			result=false;
	
		}
		return result;
	}
	private boolean isNumeric(String s) {  
	    return s.matches("[-+]?\\d*\\.?\\d+");  
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public String getEnglishFleet() {
		return englishFleet;
	}

	public void setEnglishFleet(String englishFleet) {
		this.englishFleet = englishFleet;
	}

	public String getWelshFleet() {
		return welshFleet;
	}

	public void setWelshFleet(String welshFleet) {
		this.welshFleet = welshFleet;
	}

	public String getEnglishMulti() {
		return englishMulti;
	}

	public void setEnglishMulti(String englishMulti) {
		this.englishMulti = englishMulti;
	}

	public String getWelshMulti() {
		return welshMulti;
	}

	public void setWelshMulti(String welshMulti) {
		this.welshMulti = welshMulti;
	}

	public String getEnglishUnsorted() {
		return englishUnsorted;
	}

	public void setEnglishUnsorted(String englishUnsorted) {
		this.englishUnsorted = englishUnsorted;
	}

	public String getWelshUnsorted() {
		return welshUnsorted;
	}

	public void setWelshUnsorted(String welshUnsorted) {
		this.welshUnsorted = welshUnsorted;
	}

	public String getEnglishSorted() {
		return englishSorted;
	}

	public void setEnglishSorted(String englishSorted) {
		this.englishSorted = englishSorted;
	}

	public String getWelshSorted() {
		return welshSorted;
	}

	public void setWelshSorted(String welshSorted) {
		this.welshSorted = welshSorted;
	}

	public String getEnglishClerical() {
		return englishClerical;
	}

	public void setEnglishClerical(String englishClerical) {
		this.englishClerical = englishClerical;
	}

	public String getWelshClerical() {
		return welshClerical;
	}

	public void setWelshClerical(String welshClerical) {
		this.welshClerical = welshClerical;
	}

	public String getEnglishReject() {
		return englishReject;
	}

	public void setEnglishReject(String englishReject) {
		this.englishReject = englishReject;
	}

	public String getWelshReject() {
		return welshReject;
	}

	public void setWelshReject(String welshReject) {
		this.welshReject = welshReject;
	}

	public String getEnglishReprint() {
		return englishReprint;
	}

	public void setEnglishReprint(String englishReprint) {
		this.englishReprint = englishReprint;
	}

	public String getWelshReprint() {
		return welshReprint;
	}

	public void setWelshReprint(String welshReprint) {
		this.welshReprint = welshReprint;
	}

	public String getMailingSite() {
		return mailingSite;
	}

	public void setMailingSite(String mailingSite) {
		this.mailingSite = mailingSite;
	}

	public String getMinimumMailsort() {
		return minimumMailsort;
	}

	public void setMinimumMailsort(String minimumMailsort) {
		this.minimumMailsort = minimumMailsort;
	}

	public String getMailsortProduct() {
		return mailsortProduct;
	}

	public void setMailsortProduct(String mailsortProduct) {
		this.mailsortProduct = mailsortProduct;
	}
	
}
