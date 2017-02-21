package uk.gov.dvla.osg.batch;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import uk.gov.dvla.osg.common.classes.Customer;
import uk.gov.dvla.osg.common.classes.PostageConfiguration;
import uk.gov.dvla.osg.common.classes.ProductionConfiguration;
import uk.gov.dvla.osg.common.classes.SelectorLookup;

public class BatchEngine {
	private static final Logger LOGGER = LogManager.getLogger();
	private List<Customer> input;
	private ProductionConfiguration prodConfig;
	private PostageConfiguration postConfig;
	private String parentJid;
	private String product;
	
	public BatchEngine(String parentJid, List<Customer> input, ProductionConfiguration prodConfig, PostageConfiguration postConfig, String product){
		this.parentJid=parentJid;
		this.input=input;
		this.prodConfig=prodConfig;
		this.postConfig=postConfig;
		this.product=product;
	}
	
	public void batch(){
		
		
		int j =1;
		int k =1;
		boolean firstCustomer = true;
		Customer prev = null;
		int batchMax = 0;
		int pageCount =0;
		int cusIdx = 0;
		for (Customer customer : input){
			pageCount = pageCount + customer.getNoOfPages();
			if(firstCustomer){
				prev = customer;
				firstCustomer=false;
			}
			
			batchMax = getBatchMax(customer.getLang(), customer.getBatchType());
			
			if( (prev.getLang().equals(customer.getLang()) ) && 
					(prev.getBatchType().equals(customer.getBatchType())) &&
					(prev.getSubBatch().equals(customer.getSubBatch())) &&
					(prev.getSite().equals(customer.getSite())) &&
					(pageCount < batchMax)  ){
				//SAME BATCH
				customer.setJid(parentJid + "." + j);
			}else{
				//NEW BATCH
				if( isAdjustmentRequiredForLast25(cusIdx) ){
					LOGGER.info("Adjustment required for customer '{}'",customer.getDocRef());
					j ++;
					k=1;
					pageCount = customer.getNoOfPages();
					customer.setJid(parentJid + "." + j);
				} else {
					j ++;
					k=1;
					pageCount = customer.getNoOfPages();
					customer.setJid(parentJid + "." + j);
				}
			}
			prev = customer;
			customer.setBatchSequence(j);
			customer.setSequence(k);
			k++;
			cusIdx ++;
		}
		
	}
	
	private int getBatchMax(String lang, String batchType) {
		int batchMax =0;
		if( "E".equalsIgnoreCase(lang) ){
			switch (batchType) {
				case "SORTED" : batchMax = prodConfig.getBatchMaxEnglishSorted();
				break;
				case "SORTING" : batchMax = prodConfig.getBatchMaxEnglishSorting();
				break;
				case "UNSORTED" : batchMax = prodConfig.getBatchMaxEnglishUnsorted();
				break;
				case "FLEET" : batchMax = prodConfig.getBatchMaxEnglishFleet();
				break;
				case "CLERICAL" : batchMax = prodConfig.getBatchMaxEnglishClerical();
				break;
				case "MULTI" : batchMax = prodConfig.getBatchMaxEnglishMulti();
				break;
			}
		} else {
			switch (batchType) {
				case "SORTED" : batchMax = prodConfig.getBatchMaxWelshSorted();
				break;
				case "SORTING" : batchMax = prodConfig.getBatchMaxWelshSorting();
				break;
				case "UNSORTED" : batchMax = prodConfig.getBatchMaxWelshUnsorted();
				break;
				case "FLEET" : batchMax = prodConfig.getBatchMaxWelshFleet();
				break;
				case "CLERICAL" : batchMax = prodConfig.getBatchMaxWelshClerical();
				break;
				case "MULTI" : batchMax = prodConfig.getBatchMaxWelshMulti();
				break;
			}
		}
		return batchMax;
	}

	private boolean isAdjustmentRequiredForLast25(int idx){
		idx = idx -1;
		boolean result = false;
		if( "UNSORTED".equalsIgnoreCase(product) ){
			result = false;
		} else {
			if( postConfig.getUkmBatchTypes().contains(input.get(idx).getBatchType()) ){
				if( idx < (postConfig.getUkmMinimumTrayVolume()-1) ){
					LOGGER.fatal("{} batch has less than {} mailpieces in tray.",input.get(idx).getBatchType(),postConfig.getUkmMinimumTrayVolume());
					System.exit(1);
				} else {
					if( input.get(idx - (postConfig.getUkmMinimumTrayVolume()-1) ).getMsc().equalsIgnoreCase(input.get(idx).getMsc() ) ){
						result = false;
					} else {
						result = true;
					}
				}
			} else {
				result = false;
			}
		}
		return result;
		
	}
	
}
