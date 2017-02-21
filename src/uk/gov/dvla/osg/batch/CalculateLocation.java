package uk.gov.dvla.osg.batch;

import java.text.Collator;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import uk.gov.dvla.osg.common.classes.Customer;
import uk.gov.dvla.osg.common.classes.ProductionConfiguration;
import uk.gov.dvla.osg.common.classes.SelectorLookup;

public class CalculateLocation {
	private static final Logger LOGGER = LogManager.getLogger();
	private List<Customer> input;
	private SelectorLookup lookup;
	private ProductionConfiguration props;
	
	public CalculateLocation(List<Customer> input, SelectorLookup lookup, ProductionConfiguration props){
		this.input=input;
		this.lookup=lookup;
		this.props=props;
	}
	
	public void calculate(){
		
		List<Customer> sortedE = new ArrayList<Customer>();
		List<Customer> unsortedE = new ArrayList<Customer>();
		List<Customer> fleetE = new ArrayList<Customer>();
		List<Customer> clericalE = new ArrayList<Customer>(); 
		List<Customer> multiE = new ArrayList<Customer>();
		List<Customer> sortedW = new ArrayList<Customer>();
		List<Customer> unsortedW = new ArrayList<Customer>();
		List<Customer> fleetW = new ArrayList<Customer>();
		List<Customer> clericalW = new ArrayList<Customer>(); 
		List<Customer> multiW = new ArrayList<Customer>();
		List<Customer> sortingE = new ArrayList<Customer>();
		List<Customer> sortingW = new ArrayList<Customer>();
		
		for(Customer customer : input){

			switch (customer.getBatchType()) {
				case "SORTED":  
					if("E".equals(customer.getLang())){
						sortedE.add(customer);
					}else{
						sortedW.add(customer);
					};
				break;
				case "UNSORTED": 
					if("E".equals(customer.getLang())){
						unsortedE.add(customer);
					}else{
						unsortedW.add(customer);
					};
				break; 
				case "FLEET": 
					if("E".equals(customer.getLang())){
						fleetE.add(customer);
					}else{
						fleetW.add(customer);
					};
				break;
				case "CLERICAL": 
					if("E".equals(customer.getLang())){
						clericalE.add(customer);
					}else{
						clericalW.add(customer);
					};
				break;
				case "MULTI":
					if("E".equals(customer.getLang())){
						multiE.add(customer);
					}else{
						multiW.add(customer);
					};
				break;
				case "SORTING":
					if("E".equals(customer.getLang())){
						sortingE.add(customer);
					}else{
						sortingW.add(customer);
					};
				break;
			}
		}
		
		LOGGER.info("Batch contains:\nSORTED E{} W{}\nUNSORTED E{} W{}\nFLEET E{} W{}\nCLERICAL E{} W{}\nMULTI E{} W{}\nSORTING E{} W{}",
				sortedE.size(),sortedW.size(), unsortedE.size(),unsortedW.size(),fleetE.size(),fleetW.size(),clericalE.size(),clericalW.size(),multiE.size(),multiW.size(),sortingE.size(),sortingW.size());
		int sortedCount = 0;
		int unsortedCount = 0;
		Set<String> eFleetGroupsToFf = null;
		Set<String> eClericalGroupsToFf = null;
		Set<String> eMultiGroupsToFf = null;
		Set<String> wFleetGroupsToFf = null;
		Set<String> wClericalGroupsToFf = null;
		Set<String> wMultiGroupsToFf = null;
		boolean firstCustomer = true;
		Integer eSortedToFf = null;
		Integer wSortedToFf = null;
		Integer eUnsortedToFf = null;
		Integer wUnsortedToFf = null;
		Integer eSortingtoFf = null;
		Integer wSortingtoFf = null;

		
		for(Customer customer : input){
			if(firstCustomer && lookup.get(customer.getSelectorRef()) == null){
				LOGGER.fatal("The reference '{}' couldn't be found in lookup '{}' for customer with doc ref={}",customer.getSelectorRef(),lookup.getFile(),customer.getDocRef());
				System.exit(1);
				firstCustomer =false;
			}
			if( "SORTED".equals(customer.getBatchType()) ){
				if( "E".equals(customer.getLang()) ){
					if( isNumeric(props.getEnglishSorted()) ){
						if(eSortedToFf == null){
							eSortedToFf = (int) ( sortedE.size() * ( (float)Integer.parseInt(props.getEnglishSorted()) / 100 ) );
							sortedCount = 0;
							LOGGER.info("Size of english sorted= {}, FF set to {}",sortedE.size(),eSortedToFf );
						}
						if (sortedCount < eSortedToFf){
							customer.setSite("f");
							sortedCount ++;
						}else{
							customer.setSite("m");
						}
					} else {
						if("M".equalsIgnoreCase(props.getEnglishSorted()) ){
							customer.setSite("m");
						} else {
							customer.setSite("f");
						}
					}
				}else{
					if( isNumeric(props.getWelshSorted()) ){
						if(wSortedToFf == null){
							wSortedToFf = (int) ( sortedW.size() * ( (float)Integer.parseInt(props.getWelshSorted()) / 100 ) );
							sortedCount = 0;
							LOGGER.info("Size of welsh sorted= {}, FF set to {}",sortedW.size(),wSortedToFf );
						}
						if (sortedCount < wSortedToFf){
							customer.setSite("f");
							sortedCount ++;
						}else{
							customer.setSite("m");
						}
					} else {
						if("M".equalsIgnoreCase(props.getWelshSorted()) ){
							customer.setSite("m");
						} else {
							customer.setSite("f");
						}
					}
				}
			}
			if( "UNSORTED".equals(customer.getBatchType()) ){
				if( "E".equals(customer.getLang()) ){
					if( isNumeric(props.getEnglishUnsorted()) ){
						if(eUnsortedToFf == null){
							eUnsortedToFf = (int) ( unsortedE.size() * ( (float)Integer.parseInt(props.getEnglishUnsorted()) / 100 ) );
							unsortedCount = 0;
							LOGGER.info("Size of english unsorted= {}, FF set to {}",unsortedE.size(),eUnsortedToFf );
						}
						if (unsortedCount < eUnsortedToFf){
							customer.setSite("f");
							unsortedCount ++;
						}else{
							customer.setSite("m");
						}
					} else {
						if("M".equalsIgnoreCase(props.getEnglishUnsorted()) ){
							customer.setSite("m");
						} else {
							customer.setSite("f");
						}
					}
				}else{
					if( isNumeric(props.getWelshUnsorted()) ){
						if(wUnsortedToFf == null){
							wUnsortedToFf = (int) ( unsortedW.size() * ( (float)Integer.parseInt(props.getWelshUnsorted()) / 100 ) );
							unsortedCount = 0;
							LOGGER.info("Size of welsh unsorted= {}, FF set to {}",unsortedW.size(),wUnsortedToFf );
						}
						if (unsortedCount < wUnsortedToFf){
							customer.setSite("f");
							unsortedCount ++;
						}else{
							customer.setSite("m");
						}
					} else {
						if("M".equalsIgnoreCase(props.getWelshUnsorted()) ){
							customer.setSite("m");
						} else {
							customer.setSite("f");
						}
					}
				}
			}
				
			if( "FLEET".equals(customer.getBatchType()) ){
				if( "E".equals(customer.getLang()) ){
					if( isNumeric(props.getEnglishFleet()) ){
						if(eFleetGroupsToFf == null){
							int numberToFf = (int) ( fleetE.size() * ( (float)Integer.parseInt(props.getEnglishFleet()) / 100 ) );
							eFleetGroupsToFf = calculateGroupLocation(customer.getBatchType() + customer.getLang(),fleetE, numberToFf);
						}
						if (eFleetGroupsToFf.contains(customer.getGroupId())){
							customer.setSite("f");
						}else{
							customer.setSite("m");
						}
					} else {
						if( "M".equalsIgnoreCase(props.getEnglishFleet()) ){
							customer.setSite("m");
						}else{
							customer.setSite("f");
						}
					}
				} else {
					if( isNumeric(props.getWelshFleet()) ){
						if(wFleetGroupsToFf == null){
							int numberToFf = (int) ( fleetW.size() * ( (float)Integer.parseInt(props.getWelshFleet()) / 100 ) );
							wFleetGroupsToFf = calculateGroupLocation(customer.getBatchType() + customer.getLang(),fleetW, numberToFf);
						}
						if (wFleetGroupsToFf.contains(customer.getGroupId())){
							customer.setSite("f");
						}else{
							customer.setSite("m");
						}
					} else {
						if( "M".equalsIgnoreCase(props.getWelshFleet()) ){
							customer.setSite("m");
						}else{
							customer.setSite("f");
						}
					}
				}
			}
			if( "CLERICAL".equals(customer.getBatchType()) ){
				if( "E".equals(customer.getLang()) ){
					if( isNumeric(props.getEnglishClerical()) ){
						if(eClericalGroupsToFf == null){
							int numberToFf = (int) ( clericalE.size() * ( (float)Integer.parseInt(props.getEnglishClerical()) / 100 ) );
							eClericalGroupsToFf = calculateGroupLocation(customer.getBatchType() + customer.getLang(),clericalE, numberToFf);
						}
						if (eClericalGroupsToFf.contains(customer.getGroupId())){
							customer.setSite("f");
						}else{
							customer.setSite("m");
						}
					} else {
						if( "M".equalsIgnoreCase(props.getEnglishClerical()) ){
							customer.setSite("m");
						}else{
							customer.setSite("f");
						}
					}
				} else {
					if( isNumeric(props.getWelshClerical()) ){
						if(wClericalGroupsToFf == null){
							int numberToFf = (int) ( clericalW.size() * ( (float)Integer.parseInt(props.getWelshClerical()) / 100 ) );
							wClericalGroupsToFf = calculateGroupLocation(customer.getBatchType() + customer.getLang(),fleetW, numberToFf);
						}
						if (wClericalGroupsToFf.contains(customer.getGroupId())){
							customer.setSite("f");
						}else{
							customer.setSite("m");
						}
					} else {
						if( "M".equalsIgnoreCase(props.getWelshClerical()) ){
							customer.setSite("m");
						}else{
							customer.setSite("f");
						}
					}
				}
			}
			
			if( "SORTING".equals(customer.getBatchType()) ){
				if( "E".equals(customer.getLang()) ){
					if( isNumeric(props.getEnglishSorting()) ){
						if(eSortingtoFf == null){
							eSortingtoFf = (int) ( sortedE.size() * ( (float)Integer.parseInt(props.getEnglishSorting()) / 100 ) );
							sortedCount = 0;
							LOGGER.info("Size of english sorting= {}, FF set to {}",sortingE.size(),eSortingtoFf );
						}
						if (sortedCount < eSortingtoFf){
							customer.setSite("f");
							sortedCount ++;
						}else{
							customer.setSite("m");
						}
					} else {
						if("M".equalsIgnoreCase(props.getEnglishSorting()) ){
							customer.setSite("m");
						} else {
							customer.setSite("f");
						}
					}
				}else{
					if( isNumeric(props.getWelshSorting()) ){
						if(wSortingtoFf == null){
							wSortingtoFf = (int) ( sortedW.size() * ( (float)Integer.parseInt(props.getWelshSorting()) / 100 ) );
							sortedCount = 0;
							LOGGER.info("Size of welsh sorting= {}, FF set to {}",sortingW.size(),wSortingtoFf );
						}
						if (sortedCount < wSortingtoFf){
							customer.setSite("f");
							sortedCount ++;
						}else{
							customer.setSite("m");
						}
					} else {
						if("M".equalsIgnoreCase(props.getWelshSorting()) ){
							customer.setSite("m");
						} else {
							customer.setSite("f");
						}
					}
				}
			}

			if( "MULTI".equals(customer.getBatchType()) ){
				if( "E".equals(customer.getLang()) ){
					if( isNumeric(props.getEnglishMulti()) ){
						if(eMultiGroupsToFf == null){
							int numberToFf = (int) ( multiE.size() * ( (float)Integer.parseInt(props.getEnglishMulti()) / 100 ) );
							eMultiGroupsToFf = calculateGroupLocation(customer.getBatchType() + customer.getLang(),multiE, numberToFf);
						}
						if (eMultiGroupsToFf.contains(customer.getGroupId())){
							customer.setSite("f");
						}else{
							customer.setSite("m");
						}
					} else {
						if( "M".equalsIgnoreCase(props.getEnglishMulti()) ){
							customer.setSite("m");
						}else{
							customer.setSite("f");
						}
					}
				} else {
					if( isNumeric(props.getWelshMulti()) ){
						if(wMultiGroupsToFf == null){
							int numberToFf = (int) ( multiW.size() * ( (float)Integer.parseInt(props.getWelshMulti()) / 100 ) );
							wMultiGroupsToFf = calculateGroupLocation(customer.getBatchType() + customer.getLang(),multiW, numberToFf);
						}
						if (wMultiGroupsToFf.contains(customer.getGroupId())){
							customer.setSite("f");
						}else{
							customer.setSite("m");
						}
					} else {
						if( "M".equalsIgnoreCase(props.getWelshMulti()) ){
							customer.setSite("m");
						}else{
							customer.setSite("f");
						}
					}
				}
			}
		}
	}
	
	private boolean isNumeric(String s) {  
	    return s.matches("[-+]?\\d*\\.?\\d+");  
	}
	
	private Set<String> calculateGroupLocation(String batchType,List<Customer> customers, int noToFf){
		LOGGER.info("Calculating split for batch type {}, {} customers, number to Ff={}",batchType,customers.size(),noToFf);
		Set<String> result = new HashSet<String>();
		
		//Create a list of groupId to sort
		List<String> sortedList = new ArrayList<String>();
		for(Customer customer : customers){
			sortedList.add(customer.getGroupId());
		}
		java.util.Collections.sort(sortedList, Collator.getInstance());
		
		//Create lookup list to determine which group should the split occur after
		Map<Integer,String> grpLookup = new HashMap<Integer,String>();
		int i = 0;
		for(String grpId : sortedList){
			grpLookup.put(i, grpId);
			i ++;
		}
		//Add the groups to the result
		for( int j = 0; j <= noToFf ; j++ ){
			result.add(grpLookup.get(j));
		}
		
		return result;
	}
	

}
