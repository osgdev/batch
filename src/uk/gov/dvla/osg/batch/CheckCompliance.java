package uk.gov.dvla.osg.batch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import uk.gov.dvla.osg.common.classes.Customer;
import uk.gov.dvla.osg.common.classes.PostageConfiguration;
import uk.gov.dvla.osg.common.classes.ProductionConfiguration;

public class CheckCompliance {
	private static final Logger LOGGER = LogManager.getLogger();
	private ArrayList<Customer> customers;
	private ProductionConfiguration prodConfig;
	private PostageConfiguration postConfig;
	private float compliance, maxBadDps;
	private int totalMailsortCount, badDpsCount, goodDpsCount, percentage;
	private Set<String> uniqueMscs;
	private Map<String,Integer> presLookup;
	
	public CheckCompliance(ArrayList<Customer> customers, ProductionConfiguration prodConfig,PostageConfiguration postConfig, Map<String, Integer> presLookup){
		this.customers=customers;
		this.prodConfig=prodConfig;
		this.postConfig=postConfig;
		this.presLookup=presLookup;
		uniqueMscs= new HashSet<String>();
		
		checkMscGroups();
		calculate();
	}
	
	//This calculates DPS compliance and the maximum default DPS permitted
	private void calculate() {
		totalMailsortCount = 0;
		badDpsCount = 0;
		goodDpsCount = 0;
		
		if(!( "UNSORTED".equalsIgnoreCase(prodConfig.getMailsortProduct()) ) ){
			for(Customer cus : customers){
				if(postConfig.getUkmBatchTypes().contains(cus.getBatchType()) ){
					
					totalMailsortCount ++;
					if( ( cus.getDps()==null ) || ( cus.getDps().isEmpty() ) || ( "9Z".equalsIgnoreCase(cus.getDps()) )  ){
						badDpsCount ++;
					}else{
						goodDpsCount ++;
					}
				}
			}
			percentage = 100 - postConfig.getUkmMinimumCompliance();
			maxBadDps = (((float)goodDpsCount / 100) * (float) percentage) -1;
			compliance = 100 - ( ((float) badDpsCount / (float)goodDpsCount) * 100);
			
			LOGGER.info("Run total={}, total mailsort count={}, good DPS count={}, bad DPS count={}, maximum permitted default DPS={}, compliance level={} minimum compliance set to {}",customers.size(),totalMailsortCount,goodDpsCount,badDpsCount,maxBadDps,compliance,postConfig.getUkmMinimumCompliance());

		}else{
			LOGGER.info("Mailsort product set to UNSORTED in config '{}' returning 0",prodConfig.getFilename());
			compliance = 0f;
			maxBadDps = 0;
		}
	}
	
	private void checkMscGroups(){
		ArrayList<String> mscs = new ArrayList<String>();
		ArrayList<String> mscsToAdjust = new ArrayList<String>();
		if( !( "UNSORTED".equalsIgnoreCase(prodConfig.getMailsortProduct()) ) ){
			for(Customer cus : customers){
				if( postConfig.getUkmBatchTypes().contains(cus.getBatchType()) && "X".equalsIgnoreCase(cus.getEog()) ){
					uniqueMscs.add(cus.getLang() + cus.getBatchType() + cus.getSubBatch() + cus.getMsc());
					mscs.add(cus.getLang() + cus.getBatchType() + cus.getSubBatch() + cus.getMsc());
				}
			}
			int occurrences =0;
			for(String msc : uniqueMscs){
				occurrences = Collections.frequency(mscs, msc);
				if( occurrences < postConfig.getUkmMinimumTrayVolume() ){
					mscsToAdjust.add(msc);
					LOGGER.info("MSC '{}' has only {} items, minimum volume {}",msc,occurrences,postConfig.getUkmMinimumTrayVolume());
				}
			}
			
			if( mscsToAdjust.size() > 0 ){
				LOGGER.info("Adjusting {} mscs",mscsToAdjust.size());
				for(Customer cus : customers){
					if( mscsToAdjust.contains(cus.getLang() + cus.getBatchType() + cus.getSubBatch() + cus.getMsc()) ){
						cus.updateBatchType("UNSORTED", presLookup);
						cus.setEog("X");
						cus.setPresentationPriority(presLookup.get("UNSORTED"));
					}
				}
			}
		}
	}
	

	public float getDpsAccuracy(){
		return this.compliance;
	}
	
	public float getMaximumDefaultDps(){
		return this.maxBadDps;
	}
	
	public int getTotalMailsortCount(){
		return this.totalMailsortCount;
	}

}
