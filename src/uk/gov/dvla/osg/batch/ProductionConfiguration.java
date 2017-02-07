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
		welshReprint, mailingSite, minimumMailsort, mailsortProduct, englishSorting, welshSorting;
	private int batchMaxEnglishFleet, batchMaxWelshFleet,batchMaxEnglishMulti, batchMaxWelshMulti, 
		batchMaxEnglishUnsorted, batchMaxWelshUnsorted,batchMaxEnglishSorted, batchMaxWelshSorted, 
		batchMaxEnglishClerical, batchMaxWelshClerical, batchMaxEnglishReject, batchMaxWelshReject, 
		batchMaxEnglishReprint, batchMaxWelshReprint, batchMaxEnglishSorting , batchMaxWelshSorting;
	
	
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
			    	} else if ( "site.english.sorting".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.englishSorting=value;
			    	} else if ( "site.welsh.sorting".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.welshSorting=value;
			    	} else if ( "batchMax.english.fleet".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.batchMaxEnglishFleet=Integer.parseInt(value);
			    	} else if ( "batchMax.welsh.fleet".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.batchMaxWelshFleet=Integer.parseInt(value);
			    	} else if ( "batchMax.english.multi".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.batchMaxEnglishMulti=Integer.parseInt(value);
			    	} else if ( "batchMax.welsh.multi".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.batchMaxWelshMulti=Integer.parseInt(value);
			    	} else if ( "batchMax.english.unsorted".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.batchMaxEnglishUnsorted=Integer.parseInt(value);
			    	} else if ( "batchMax.welsh.unsorted".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.batchMaxWelshUnsorted=Integer.parseInt(value);
			    	} else if ( "batchMax.english.sorted".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.batchMaxEnglishSorted=Integer.parseInt(value);
			    	} else if ( "batchMax.welsh.sorted".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.batchMaxWelshSorted=Integer.parseInt(value);
			    	} else if ( "batchMax.english.clerical".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.batchMaxEnglishClerical=Integer.parseInt(value);
			    	} else if ( "batchMax.welsh.clerical".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.batchMaxWelshClerical=Integer.parseInt(value);
			    	} else if ( "batchMax.english.reject".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.batchMaxEnglishReject=Integer.parseInt(value);
			    	} else if ( "batchMax.welsh.reject".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.batchMaxWelshReject=Integer.parseInt(value);
			    	} else if ( "batchMax.english.reprint".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.batchMaxEnglishReprint=Integer.parseInt(value);
			    	} else if ( "batchMax.welsh.reprint".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.batchMaxWelshReprint=Integer.parseInt(value);
			    	} else if ( "batchMax.english.sorting".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.batchMaxEnglishSorting=Integer.parseInt(value);
			    	} else if ( "batchMax.welsh.sorting".equalsIgnoreCase(attribute) && isValid(attribute, value) ){
			    		this.batchMaxWelshSorting=Integer.parseInt(value);
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
		if( att.startsWith("batchMax") || att.startsWith("batchmax") || att.startsWith("BATCHMAX") ){
			if( !(isNumeric(val)) ){
				result = false;
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

	public int getBatchMaxEnglishFleet() {
		return batchMaxEnglishFleet;
	}

	public void setBatchMaxEnglishFleet(int batchMaxEnglishFleet) {
		this.batchMaxEnglishFleet = batchMaxEnglishFleet;
	}

	public int getBatchMaxWelshFleet() {
		return batchMaxWelshFleet;
	}

	public void setBatchMaxWelshFleet(int batchMaxWelshFleet) {
		this.batchMaxWelshFleet = batchMaxWelshFleet;
	}

	public int getBatchMaxEnglishMulti() {
		return batchMaxEnglishMulti;
	}

	public void setBatchMaxEnglishMulti(int batchMaxEnglishMulti) {
		this.batchMaxEnglishMulti = batchMaxEnglishMulti;
	}

	public int getBatchMaxWelshMulti() {
		return batchMaxWelshMulti;
	}

	public void setBatchMaxWelshMulti(int batchMaxWelshMulti) {
		this.batchMaxWelshMulti = batchMaxWelshMulti;
	}

	public int getBatchMaxEnglishUnsorted() {
		return batchMaxEnglishUnsorted;
	}

	public void setBatchMaxEnglishUnsorted(int batchMaxEnglishUnsorted) {
		this.batchMaxEnglishUnsorted = batchMaxEnglishUnsorted;
	}

	public int getBatchMaxWelshUnsorted() {
		return batchMaxWelshUnsorted;
	}

	public void setBatchMaxWelshUnsorted(int batchMaxWelshUnsorted) {
		this.batchMaxWelshUnsorted = batchMaxWelshUnsorted;
	}

	public int getBatchMaxEnglishSorted() {
		return batchMaxEnglishSorted;
	}

	public void setBatchMaxEnglishSorted(int batchMaxEnglishSorted) {
		this.batchMaxEnglishSorted = batchMaxEnglishSorted;
	}

	public int getBatchMaxWelshSorted() {
		return batchMaxWelshSorted;
	}

	public void setBatchMaxWelshSorted(int batchMaxWelshSorted) {
		this.batchMaxWelshSorted = batchMaxWelshSorted;
	}

	public int getBatchMaxEnglishClerical() {
		return batchMaxEnglishClerical;
	}

	public void setBatchMaxEnglishClerical(int batchMaxEnglishClerical) {
		this.batchMaxEnglishClerical = batchMaxEnglishClerical;
	}

	public int getBatchMaxWelshClerical() {
		return batchMaxWelshClerical;
	}

	public void setBatchMaxWelshClerical(int batchMaxWelshClerical) {
		this.batchMaxWelshClerical = batchMaxWelshClerical;
	}

	public int getBatchMaxEnglishReject() {
		return batchMaxEnglishReject;
	}

	public void setBatchMaxEnglishReject(int batchMaxEnglishReject) {
		this.batchMaxEnglishReject = batchMaxEnglishReject;
	}

	public int getBatchMaxWelshReject() {
		return batchMaxWelshReject;
	}

	public void setBatchMaxWelshReject(int batchMaxWelshReject) {
		this.batchMaxWelshReject = batchMaxWelshReject;
	}

	public int getBatchMaxEnglishReprint() {
		return batchMaxEnglishReprint;
	}

	public void setBatchMaxEnglishReprint(int batchMaxEnglishReprint) {
		this.batchMaxEnglishReprint = batchMaxEnglishReprint;
	}

	public int getBatchMaxWelshReprint() {
		return batchMaxWelshReprint;
	}

	public void setBatchMaxWelshReprint(int batchMaxWelshReprint) {
		this.batchMaxWelshReprint = batchMaxWelshReprint;
	}

	public String getEnglishSorting() {
		return englishSorting;
	}

	public void setEnglishSorting(String englishSorting) {
		this.englishSorting = englishSorting;
	}

	public String getWelshSorting() {
		return welshSorting;
	}

	public void setWelshSorting(String welshSorting) {
		this.welshSorting = welshSorting;
	}

	public int getBatchMaxEnglishSorting() {
		return batchMaxEnglishSorting;
	}

	public void setBatchMaxEnglishSorting(int batchMaxEnglishSorting) {
		this.batchMaxEnglishSorting = batchMaxEnglishSorting;
	}

	public int getBatchMaxWelshSorting() {
		return batchMaxWelshSorting;
	}

	public void setBatchMaxWelshSorting(int batchMaxWelshSorting) {
		this.batchMaxWelshSorting = batchMaxWelshSorting;
	}
	
	
}
