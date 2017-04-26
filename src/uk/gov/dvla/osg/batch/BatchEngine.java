package uk.gov.dvla.osg.batch;


import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import uk.gov.dvla.osg.common.classes.Customer;
import uk.gov.dvla.osg.common.classes.PapersizeLookup;
import uk.gov.dvla.osg.common.classes.PostageConfiguration;
import uk.gov.dvla.osg.common.classes.ProductionConfiguration;


public class BatchEngine {
	private static final Logger LOGGER = LogManager.getLogger();
	private List<Customer> input;
	private ProductionConfiguration prodConfig;
	private PostageConfiguration postConfig;
	private PapersizeLookup pl;
	private String parentJid;
	private int jid, jidInc, tenDigitJid, batchSequence, batchMax, pageCount;
	
	
	public BatchEngine(String parentJid, List<Customer> input, ProductionConfiguration prodConfig, PostageConfiguration postConfig, String jid , String jidInc, PapersizeLookup pl){
		this.parentJid=parentJid;
		this.input=input;
		this.prodConfig=prodConfig;
		this.postConfig=postConfig;
		this.jid=Integer.parseInt(jid);
		this.jidInc=Integer.parseInt(jidInc);
		this.pl=pl;
		batch();
	}
	
	private void batch(){
		batchSequence = 1;
		int pid =1;
		boolean firstCustomer = true;
		Customer prev = null;
		
		batchMax = 0;
		pageCount =0;
		int cusIdx = 0;
		tenDigitJid = jid + jidInc;
		
		for (Customer customer : input){
			pageCount = pageCount + customer.getNoOfPages();
			if(firstCustomer){
				prev = customer;
				firstCustomer=false;
			}
			batchMax = getBatchMax(customer.getLang(), customer.getBatchType(), customer.getPaperSize());
			
			if( isPartOfSameBatch(customer, prev) ){
				
				//SAME BATCH
				customer.setJid(parentJid + "." + batchSequence);
				customer.setTenDigitJid(tenDigitJid);
				
			}else{
				//NEW BATCH
				LOGGER.info("Creating new batch {} with start doc ref '{}'", tenDigitJid + jidInc, customer.getDocRef());
				pid = 1;
				if( adjustmentIsRequired(cusIdx) ){
					pid = adjustBatch(cusIdx);
				} else {
					if( !(customerIsEog(cusIdx - 1)) ){
						input.get(cusIdx - 1).setEog("X");
					}
				}
				
				
				
				batchSequence ++;
				tenDigitJid = tenDigitJid + jidInc;
				pageCount = customer.getNoOfPages();
				customer.setJid(parentJid + "." + batchSequence);
				customer.setTenDigitJid(tenDigitJid);

			}
			prev = customer;
			customer.setBatchSequence(batchSequence);
			customer.setSequence(pid);
			pid++;
			cusIdx ++;
		}
		
	}
	

	private boolean customerIsEog(int cusIdx) {
		boolean result = false;
		if( "X".equalsIgnoreCase(input.get(cusIdx).getEog()) ){
			result = true;
		}
		LOGGER.debug("previousBatchEndsInEog({}) returned '{}'",cusIdx ,result);
		return result;
	}

	private boolean isPartOfSameBatch(Customer customer, Customer prev){
		boolean result = false;
		if( (prev.getLang().equals(customer.getLang()) ) && 
				(prev.getBatchType().equals(customer.getBatchType())) &&
				(prev.getSubBatch().equals(customer.getSubBatch())) &&
				(prev.getSite().equals(customer.getSite())) &&
				(prev.getStationery().equals(customer.getStationery())) &&
				(pageCount <= (batchMax)) ){
			result = true;
		} else {
			result = false;
		}
		LOGGER.trace("isPartOfSameBatch({},{}) returned '{}'",customer, prev, result);
		return result;
	}
	
	private int adjustBatch(int cusIdx) {
		int result = 0;
		if( adjustmentIsRequiredForMultiWithEog(cusIdx) && adjustmentIsRequiredForBatchWithLessThanLimit(cusIdx) ){
			result = adjustMultiEogAndLessThanLimit(cusIdx);
		}else if( adjustmentIsRequiredForMultiWithEog(cusIdx) ){
			result = adjustMultiEog(cusIdx);
		}else{
			result = adjustLessThanLimit(cusIdx);
		}
		LOGGER.debug("adjustBatch({}) returned '{}'",cusIdx, result);
		return result;
	}

	private int adjustLessThanLimit(int cusIdx) {
		int result = 0;
		if( !(previousBatchHasAtLeastMscGroups( cusIdx, postConfig.getUkmMinimumTrayVolume())) ){
			result = adjustPrevBatchLessThanLimit(cusIdx);
		} else {
			result = adjustThisBatchLessThanLimit(cusIdx);
		}
		LOGGER.debug("adjustLessThanLimit({}) returned '{}'", cusIdx, result);
		return result;
	}

	private int adjustThisBatchLessThanLimit(int cusIdx) {
		int result = 0;
		int changeOfMsc = getChangeOfMscIdx(cusIdx, true);
		result = adjustBatchEndStartingFromIdx(changeOfMsc, cusIdx);
		LOGGER.debug("adjustThisBatchLessThanLimit({}) returned '{}'", cusIdx, result);
		return result;
	}

	private int adjustPrevBatchLessThanLimit(int cusIdx) {
		int result = 0;
		int changeOfMsc = getChangeOfMscIdx(cusIdx -1, false);
		result = adjustBatchStartStartingFromIdx(changeOfMsc, cusIdx);
		LOGGER.debug("adjustPrevBatchLessThanLimit({}) returned '{}'", cusIdx, result);
		return result;
	}

	private int adjustMultiEogAndLessThanLimit(int cusIdx) {
		int result = 0;
		result = adjustPrevBatchLessThanLimit(cusIdx);
		// TODO Auto-generated method stub
		LOGGER.debug("adjustMultiEogAndLessThanLimit({}) returned '{}'", cusIdx, result);
		return result;
	}

	private int adjustMultiEog(int cusIdx) {
		int result = 0; 
		int prevEog = getNextEndOfGroupStartingFromIdx(cusIdx, false);
		result = adjustBatchStartStartingFromIdx(prevEog + 1, cusIdx);
		LOGGER.debug("adjustMultiEog({}) returned '{}'",cusIdx, result);
		return result;
	}

	private boolean adjustmentIsRequired(int cusIdx) {
		boolean result = false;
		if( adjustmentIsRequiredForMultiWithEog(cusIdx) || adjustmentIsRequiredForBatchWithLessThanLimit(cusIdx) ){
			result = true;
		}
		LOGGER.debug("adjustmentIsRequired({}) returned '{}'",cusIdx, result);
		return result;
	}

	private boolean adjustmentIsRequiredForBatchWithLessThanLimit(int cusIdx) {
		boolean result = false;
		int previousBatchIdx = cusIdx - 1;
		if( isMailsortable(previousBatchIdx)  ){
			if( !(previousBatchHasAtLeastMscGroups( cusIdx, postConfig.getUkmMinimumTrayVolume())) || 
					!(currentBatchHasAtLeastMscGroups( cusIdx, postConfig.getUkmMinimumTrayVolume())) ){
				result = true;
			}
		}
		LOGGER.debug("isAdjustmentRequiredForBatchWithLessThanLimit({}) returned '{}'",cusIdx, result);
		return result;
	}

	private boolean currentBatchHasAtLeastMscGroups(int cusIdx,int minTrayVolume) {
		boolean result = false;
		if( countForwardsEogsWithSameMsc(cusIdx) >= minTrayVolume ){
			result = true;
		}
		LOGGER.debug("currentBatchHasAtLeastMscGroups({},{}) returned '{}'",cusIdx, minTrayVolume, result);
		return result;
	}

	private int countForwardsEogsWithSameMsc(int cusIdx) {
		int result = 0;
		boolean mscsMatch = true;
		String mscToMatch = input.get(cusIdx).getMsc();
		for( int i = cusIdx; mscsMatch; i++){
			if( input.get(i).getMsc().equals(mscToMatch) ){
				result ++;
			} else {
				mscsMatch = false;
			}
		}
		LOGGER.debug("countForwardsEogsWithSameMsc({}) returned '{}'",cusIdx, result);
		return result;
	}

	private boolean previousBatchHasAtLeastMscGroups(int cusIdx, int minTrayVolume) {
		boolean result = false;
		int previousBatchIdx = cusIdx - 1;
		if( countBackwardsEogsWithSameMsc(previousBatchIdx) >= minTrayVolume ){
			result = true;
		}
		LOGGER.debug("previousBatchHasAtLeastMscGroups({},{}) returned '{}'",cusIdx, minTrayVolume, result);
		return result;
	}

	private int countBackwardsEogsWithSameMsc(int previousBatchIdx) {
		int result = 0;
		boolean mscsMatch = true;
		String mscToMatch = input.get(previousBatchIdx).getMsc();
		for( int i = previousBatchIdx; mscsMatch; i--){
			if( input.get(i).getMsc().equals(mscToMatch) ){
				result ++;
			} else {
				mscsMatch = false;
			}
		}
		LOGGER.debug("countBackwardsEogsWithSameMsc({}) returned '{}'",previousBatchIdx, result);
		return result;
	}

	private boolean isMailsortable(int cusIdx) {
		boolean result = postConfig.getUkmBatchTypes().contains(getBatchType(cusIdx));
		LOGGER.debug("isMailsortable({}) returned '{}'",cusIdx, result);
		return result;
	}

	private boolean adjustmentIsRequiredForMultiWithEog(int cusIdx) {
		boolean result = false;
		if( "MULTI".equalsIgnoreCase(input.get(cusIdx).getBatchType()) && 
				"X".equalsIgnoreCase(input.get(cusIdx).getEog()) ){
			result = true;
		}
		LOGGER.debug("adjustmentRequiredForMultiWithEog({}) returned '{}'",cusIdx, result);
		return result;
	}

	private String getBatchType(int cusIdx) {
		String result = input.get(cusIdx).getBatchType();
		LOGGER.info("getPreviousBatchType({}) returned '{}'",(cusIdx), result);
		return result;
	}

	private int adjustBatchStartStartingFromIdx(int startAdjustmentIdx, int stopAdjustIdx) {
		int result =1;
		for(int adjust = startAdjustmentIdx; adjust < stopAdjustIdx;  adjust ++){
			input.get(adjust).setJid(parentJid + "." + (batchSequence + 1) );
			input.get(adjust).setTenDigitJid(tenDigitJid + jidInc);
			input.get(adjust).setSequence(result);
			result ++;
		}
		LOGGER.debug("adjustBatchStartStartingFromIdx({},{}) returned '{}'",startAdjustmentIdx, stopAdjustIdx, result);
		return result;
	}
	
	private int adjustBatchEndStartingFromIdx(int startAdjustmentIdx, int stopAdjustIdx) {
		int prevPid = input.get(startAdjustmentIdx).getSequence();
		for(int adjust = startAdjustmentIdx; adjust < stopAdjustIdx;  adjust ++){
			input.get(adjust).setJid(parentJid + "." + batchSequence );
			input.get(adjust).setTenDigitJid(tenDigitJid);
			input.get(adjust).setSequence(prevPid);
			prevPid ++;
		}
		int result = 1;
		LOGGER.debug("adjustBatchEndStartingFromIdx({},{}) returned '{}'",startAdjustmentIdx, stopAdjustIdx, result);
		return 1;
	}

	private int getNextEndOfGroupStartingFromIdx(int cusIdx, boolean ascending) {
		int result = 0;
		boolean continueLooking = true;
		if(ascending){
			for( result = cusIdx; continueLooking ; result ++ ){
				if( "X".equalsIgnoreCase(input.get(result).getEog()) ){
					continueLooking=false;
					break;
				}
			}
		} else {
			for( result = cusIdx; continueLooking ; result -- ){
				if( "X".equalsIgnoreCase(input.get(result).getEog()) ){
					continueLooking=false;
					break;
				}
			}
		}
		LOGGER.debug("getNextEndOfGroupStartingFromIdx({},{}) returned '{}'",cusIdx, ascending, result);
		return result;
	}
	
	private int getChangeOfMscIdx(int cusIdx, boolean ascending) {
		int result = 0;
		String currentMsc = input.get(cusIdx).getMsc();
		boolean continueLooking = true;
		
		if(ascending){
			for( result = cusIdx; continueLooking ; result ++ ){
				if( !(currentMsc.equalsIgnoreCase(input.get(result).getMsc())) ){
					continueLooking=false;
					result--;
					break;
				}
			}
		} else {
			for( result = cusIdx; continueLooking ; result -- ){
				if( !(currentMsc.equalsIgnoreCase(input.get(result).getMsc())) ){
					continueLooking=false;
					result++;
					break;
				}
			}
		}
		LOGGER.debug("getChangeOfMscIdx({},{}) returned '{}'", cusIdx, ascending, result );
		return result;
	}

	private int getBatchMax(String lang, String batchType, String paperSize) {
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
		if( pl.getLookup().containsKey(paperSize) ){
			batchMax = (int) (batchMax * pl.getLookup().get(paperSize).getMultiplier());
		}
		return batchMax;
	}

}
