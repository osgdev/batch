package uk.gov.dvla.osg.batch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SelectorLookup {
	private static final Logger LOGGER = LogManager.getLogger(Main.class.getName());
	
	private String ref, fleet, multi, uncoded,
	coded, clerical, reject, reprint, mailsortProduct, postageConfig, filePath, presentationConfig;

	private int batchMax, mailsortThreshold;
	
	private HashMap<String, SelectorLookup> lookup = new HashMap<String, SelectorLookup>();
	
	public SelectorLookup(String file){
		this.filePath=file;
		if(new File(file).exists()){
			try (BufferedReader br = new BufferedReader(new FileReader(file))) {
				String line;
			    while ((line = br.readLine()) != null) {
			    	String[] array = line.split("\\|");
			    	if( !("SELECTOR".equals(array[0].trim())) ){
			    		lookup.put(array[0].trim(), new SelectorLookup(file, array[0].trim(),Integer.parseInt(array[1].trim()),
			    				array[2].trim(),array[3].trim(),array[4].trim(),array[5].trim(),array[6].trim(),array[7].trim(),array[8].trim(),
			    				Integer.parseInt(array[9].trim()), array[10].trim(), array[11].trim(), array[12].trim() ));
			    	}
			    }
			} catch (FileNotFoundException e) {
				LOGGER.fatal("Lookup file error: '{}'",e.getMessage());
				System.exit(1);
			} catch (IOException e) {
				LOGGER.fatal("Lookup file error: '{}'",e.getMessage());
				System.exit(1);
			}
		}else{
			LOGGER.fatal("Lookup file: '{}' doesn't exist",file);
			System.exit(1);
		}
	}
	
	public SelectorLookup(String file, String ref, int batchMax, String fleet, String multi, String uncoded,
			String coded, String clerical, String reject, String reprint,
			int mailsortThreshold, String mailsortProduct, String postageConfig, String presentationConfig){
		this.filePath=file;
		if(validateLookupEntry(ref, fleet, multi, uncoded, coded, clerical, reject, reprint)){
			this.ref=ref;
			this.fleet=fleet;
			this.multi=multi;
			this.uncoded=uncoded;
			this.coded=coded;
			this.clerical=clerical;
			this.reject=reject;
			this.reprint=reprint;
			this.mailsortProduct=mailsortProduct;
			this.postageConfig=postageConfig;
			this.batchMax=batchMax;
			this.mailsortThreshold=mailsortThreshold;
			this.presentationConfig=presentationConfig;
		}else{
			LOGGER.fatal("Validating lookup file '{}' failed on ref '{}' when processing",filePath,ref);
			System.exit(1);
		}
		
	}
	
	private boolean validateLookupEntry(String ref, String fleet, String multi, String uncoded, String coded, String clerical, String reject, String reprint){
		if( !("m".equalsIgnoreCase(fleet)) && !("f".equalsIgnoreCase(fleet)) && !("x".equalsIgnoreCase(fleet)) && !(isNumeric(fleet)) ){
			LOGGER.error("Invalid entry in lookup file '{}'. Invalid value is '{}' in fleet column for ref '{}'.",filePath, fleet, ref);
			return false;
		}
		if( !("m".equalsIgnoreCase(multi)) && !("f".equalsIgnoreCase(multi)) && !("x".equalsIgnoreCase(multi)) && !(isNumeric(multi)) ){
			LOGGER.error("Invalid entry in lookup file '{}'. Invalid value is '{}' in multi column for ref '{}'.",filePath, multi, ref);
			return false;
		}
		if( !("m".equalsIgnoreCase(uncoded)) && !("f".equalsIgnoreCase(uncoded)) && !("x".equalsIgnoreCase(uncoded)) && !(isNumeric(uncoded)) ){
			LOGGER.error("Invalid entry in lookup file '{}'. Invalid value is '{}' in uncoded column for ref '{}'.",filePath, uncoded, ref);
			return false;
		}
		if( !("m".equalsIgnoreCase(coded)) && !("f".equalsIgnoreCase(coded)) && !("x".equalsIgnoreCase(coded)) && !(isNumeric(coded)) ){
			LOGGER.error("Invalid entry in lookup file '{}'. Invalid value is '{}' in coded column for ref '{}'.",filePath, coded, ref);
			return false;
		}
		if( !("m".equalsIgnoreCase(clerical)) && !("f".equalsIgnoreCase(clerical)) && !("x".equalsIgnoreCase(clerical)) && !(isNumeric(clerical)) ){
			LOGGER.error("Invalid entry in lookup file '{}'. Invalid value is '{}' in clerical column for ref '{}'.",filePath, clerical, ref);
			return false;
		}
		if( !("m".equalsIgnoreCase(reject)) && !("f".equalsIgnoreCase(reject)) && !("x".equalsIgnoreCase(reject)) && !(isNumeric(reject)) ){
			LOGGER.error("Invalid entry in lookup file '{}'. Invalid value is '{}' in reject column for ref '{}'.",filePath, reject, ref);
			return false;
		}
		if( !("m".equalsIgnoreCase(reprint)) && !("f".equalsIgnoreCase(reprint)) && !("x".equalsIgnoreCase(reprint)) && !(isNumeric(reprint)) ){
			LOGGER.error("Invalid entry in lookup file '{}'. Invalid value is '{}' in reprint column for ref '{}'.",filePath, reprint, ref);
			return false;
		}
		if( "x".equalsIgnoreCase(fleet) && "x".equalsIgnoreCase(multi) && "x".equalsIgnoreCase(uncoded) && "x".equalsIgnoreCase(coded) && "x".equalsIgnoreCase(clerical) && "x".equalsIgnoreCase(reject) && "x".equalsIgnoreCase(reprint) ){
			LOGGER.error("All batch types set to 'X' in lookup file '{}' for reference '{}'",filePath, ref);
			return false;
		}
		if( "x".equalsIgnoreCase(uncoded) && "x".equalsIgnoreCase(coded) ){
			LOGGER.error("Coded and Uncoded columns both set to X in lookup file '{}' for reference '{}'",filePath, ref);
			return false;
		}
		
		return true;
	}
	
	private boolean isNumeric(String s) {  
	    return s.matches("[-+]?\\d*\\.?\\d+");  
	}

	public String getRef() {
		return ref;
	}

	public void setRef(String ref) {
		this.ref = ref;
	}

	public String getFleet() {
		return fleet;
	}
	
	public String getFile() {
		return filePath;
	}

	public void setFleet(String fleet) {
		this.fleet = fleet;
	}

	public String getMulti() {
		return multi;
	}

	public void setMulti(String multi) {
		this.multi = multi;
	}

	public String getUncoded() {
		return uncoded;
	}

	public void setUncoded(String uncoded) {
		this.uncoded = uncoded;
	}

	public String getCoded() {
		return coded;
	}

	public void setCoded(String coded) {
		this.coded = coded;
	}

	public String getClerical() {
		return clerical;
	}

	public void setClerical(String clerical) {
		this.clerical = clerical;
	}

	public String getReject() {
		return reject;
	}

	public void setReject(String reject) {
		this.reject = reject;
	}

	public String getReprint() {
		return reprint;
	}

	public void setReprint(String reprint) {
		this.reprint = reprint;
	}

	public String getMailsortProduct() {
		return mailsortProduct;
	}

	public void setMailsortProduct(String mailsortProduct) {
		this.mailsortProduct = mailsortProduct;
	}

	public String getPresentationConfig() {
		return presentationConfig;
	}

	public void setPresentationConfig(String presentationConfig) {
		this.presentationConfig = presentationConfig;
	}
	
	public String getPostageConfig() {
		return postageConfig;
	}

	public void setPostageConfig(String postageConfig) {
		this.postageConfig = postageConfig;
	}

	public int getBatchMax() {
		return batchMax;
	}

	public void setBatchMax(int batchMax) {
		this.batchMax = batchMax;
	}

	public int getMailsortThreshold() {
		return mailsortThreshold;
	}

	public void setMailsortThreshold(int mailsortThreshold) {
		this.mailsortThreshold = mailsortThreshold;
	}
	
	public SelectorLookup get(String ref){
		return lookup.get(ref);
	}
}
