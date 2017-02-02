package uk.gov.dvla.osg.batch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SelectorLookup {
	private static final Logger LOGGER = LogManager.getLogger(Main.class.getName());
	private Properties CONFIG;
	
	private String ref, productionConfig, postageConfig, filePath, presentationConfig;

	private int batchMax;
	
	private HashMap<String, SelectorLookup> lookup = new HashMap<String, SelectorLookup>();
	
	public SelectorLookup(String file, Properties props){
		this.filePath=file;
		this.CONFIG=props;
		if(new File(file).exists()){
			try (BufferedReader br = new BufferedReader(new FileReader(file))) {
				String line;
			    while ((line = br.readLine()) != null) {
			    	String[] array = line.split("\\|");
			    	if( !("SELECTOR".equals(array[0].trim())) ){
			    		lookup.put(array[0].trim(), new SelectorLookup(file, array[0].trim(),Integer.parseInt(array[1].trim()),
			    				array[2].trim(),array[3].trim(),array[4].trim() ));
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
	
	public SelectorLookup(String file, String ref, int batchMax, String productionConfig, String postageConfig, String presentationConfig){
		this.filePath=file;
		if(validateLookupEntry( batchMax, productionConfig, postageConfig, presentationConfig)){
			this.ref=ref;
			this.batchMax=batchMax;
			this.postageConfig=postageConfig;
			this.productionConfig = productionConfig;
			this.presentationConfig=presentationConfig;
		}else{
			LOGGER.fatal("Validating lookup file '{}' failed on ref '{}' when processing",filePath,ref);
			System.exit(1);
		}
		
	}
	
	private boolean validateLookupEntry(int batchMax, String productionConfig, String postageConfig, String presentationConfig){
		boolean result = true;
		if( !(isNumeric("" + batchMax)) ){
			result = false;
			LOGGER.error("Field 'batchMax' has erroneous value '{}'",batchMax);
		}
		
		if( !(new File(CONFIG.getProperty("productionConfigPath") + productionConfig + CONFIG.getProperty("productionFileSuffix")).exists()) ){
			result = false;
			LOGGER.error("File '{}' doesn't exist",CONFIG.getProperty("productionConfigPath") + productionConfig + CONFIG.getProperty("productionFileSuffix"));
		}
		if( !(new File(CONFIG.getProperty("postageConfigPath") + postageConfig + CONFIG.getProperty("postageFileSuffix")).exists()) ){
			result = false;
			LOGGER.error("File '{}' doesn't exist",CONFIG.getProperty("postageConfigPath") + postageConfig + CONFIG.getProperty("postageFileSuffix"));
		}
		if( !(new File(CONFIG.getProperty("presentationPriorityConfigPath") + presentationConfig + CONFIG.getProperty("presentationPriorityFileSuffix")).exists()) ){
			result = false;
			LOGGER.error("File '{}' doesn't exist",CONFIG.getProperty("presentationPriorityConfigPath") + presentationConfig + CONFIG.getProperty("presentationPriorityFileSuffix"));
		}
		
		return result;
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

	public String getFile() {
		return filePath;
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

	public SelectorLookup get(String ref){
		return lookup.get(ref);
	}

	public String getProductionConfig() {
		return productionConfig;
	}

	public void setProductionConfig(String productionConfig) {
		this.productionConfig = productionConfig;
	}
}
