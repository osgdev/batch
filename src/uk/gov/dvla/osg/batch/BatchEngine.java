package uk.gov.dvla.osg.batch;


import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import uk.gov.dvla.osg.common.classes.Customer;
import uk.gov.dvla.osg.common.classes.PostageConfiguration;
import uk.gov.dvla.osg.common.classes.ProductionConfiguration;


public class BatchEngine {
	private static final Logger LOGGER = LogManager.getLogger();
	private List<Customer> input;
	private ProductionConfiguration prodConfig;
	private PostageConfiguration postConfig;
	private String parentJid;
	private int jid, jidInc;
	
	public BatchEngine(String parentJid, List<Customer> input, ProductionConfiguration prodConfig, PostageConfiguration postConfig, String jid , String jidInc){
		this.parentJid=parentJid;
		this.input=input;
		this.prodConfig=prodConfig;
		this.postConfig=postConfig;
		this.jid=Integer.parseInt(jid);
		this.jidInc=Integer.parseInt(jidInc);
		batch();
	}
	
	private void batch(){
		int batchSequence =1;
		int pid =1;
		boolean firstCustomer = true;
		Customer prev = null;
		int batchMax = 0;
		int pageCount =0;
		int cusIdx = 0;
		int mscCounter = 0;
		int tenDigitJid = jid + jidInc;
		int lastEogIdx = 0;
		
		for (Customer customer : input){
			pageCount = pageCount + customer.getNoOfPages();
			if(firstCustomer){
				prev = customer;
				firstCustomer=false;
			}
			
			if( !(customer.getMsc().equals(prev.getMsc())) ){
				mscCounter = 0;
			}
			
			if( "X".equalsIgnoreCase(customer.getEog()) ){
				mscCounter ++;
				lastEogIdx=cusIdx;
			}
			
			batchMax = getBatchMax(customer.getLang(), customer.getBatchType());
			
			if( (prev.getLang().equals(customer.getLang()) ) && 
					(prev.getBatchType().equals(customer.getBatchType())) &&
					(prev.getSubBatch().equals(customer.getSubBatch())) &&
					(prev.getSite().equals(customer.getSite())) &&
					(prev.getStationery().equals(customer.getStationery())) &&
					(pageCount < (batchMax)) ){
				
				//SAME BATCH
				customer.setJid(parentJid + "." + batchSequence);
				customer.setTenDigitJid(tenDigitJid);
				
			}else{
				//NEW BATCH
				LOGGER.info("Creating new batch..");
				pid = 1;
				if( (mscCounter < postConfig.getUkmMinimumTrayVolume()) && 
						postConfig.getUkmBatchTypes().contains(customer.getBatchType())){
					LOGGER.debug("Attempting to batch at idx '{}', customer doc ref '{}'. Number of MSC in group = {} need a minimum of {}",cusIdx, customer.getDocRef(), mscCounter, postConfig.getUkmMinimumTrayVolume());	
					int startAdjustmentIdx = getChangeOfMscIdx(cusIdx, customer.getMsc() ,false);
					pid = adjustBatchStartingFromIdx(startAdjustmentIdx, cusIdx, batchSequence, tenDigitJid, jidInc);
				}
				
				if( !("X".equalsIgnoreCase(customer.getEog())) ){
					LOGGER.debug("Attempting to batch at idx '{}', customer doc ref '{}'. Customer is not EOG and is in a {} batch",cusIdx, customer.getDocRef(), customer.getBatchType());
					if( postConfig.getUkmBatchTypes().contains(customer.getBatchType()) ){
						int startAdjustmentIdx = getNextEndOfGroupStartingFromIdx(cusIdx, false);
						pid = adjustBatchStartingFromIdx(startAdjustmentIdx, cusIdx, batchSequence, tenDigitJid, jidInc);
					} else {
						customer.setEog("X");
					}
				}

				batchSequence ++;
				tenDigitJid = tenDigitJid + jidInc;
				pageCount = 0;
				customer.setJid(parentJid + "." + batchSequence);
				customer.setTenDigitJid(tenDigitJid);
				mscCounter=0;
			}
			prev = customer;
			customer.setBatchSequence(batchSequence);
			customer.setSequence(pid);
			pid++;
			cusIdx ++;
		}
		
	}
	
	private int adjustBatchStartingFromIdx(int startAdjustmentIdx, int stopAdjustIdx, int batchSequence, int tenDigitJid, int jidInc) {
		int newPid =1;
		for(int adjust = startAdjustmentIdx; adjust < stopAdjustIdx;  adjust ++){
			input.get(adjust).setJid(parentJid + "." + (batchSequence + 1) );
			input.get(adjust).setTenDigitJid(tenDigitJid + jidInc);
			input.get(adjust).setSequence(newPid);
			newPid ++;
		}
		return newPid;
	}

	private int getNextEndOfGroupStartingFromIdx(int cusIdx, boolean ascending) {
		int count = 0;
		boolean continueLooking = true;
		
		if(ascending){
			for( count = cusIdx; continueLooking ; count ++ ){
				if( "X".equalsIgnoreCase(input.get(count).getEog()) ){
					continueLooking=false;
					break;
				}
			}
		} else {
			for( count = cusIdx; continueLooking ; count -- ){
				if( "X".equalsIgnoreCase(input.get(count).getEog()) ){
					continueLooking=false;
					break;
				}
			}
		}
		return count;
	}
	
	private int getChangeOfMscIdx(int cusIdx, String currentMsc, boolean ascending) {
		int count = 0;
		boolean continueLooking = true;
		
		if(ascending){
			for( count = cusIdx; continueLooking ; count ++ ){
				if( !(currentMsc.equalsIgnoreCase(input.get(count).getMsc())) ){
					continueLooking=false;
					break;
				}
			}
		} else {
			for( count = cusIdx; continueLooking ; count -- ){
				if( !(currentMsc.equalsIgnoreCase(input.get(count).getMsc())) ){
					continueLooking=false;
					break;
				}
			}
		}
		return count;
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

}
