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

public class CalculateLocation {
	private static final Logger LOGGER = LogManager.getLogger(Main.class.getName());
	private List<Customer> input;
	private SelectorLookup lookup;
	private Properties props;
	
	public CalculateLocation(List<Customer> input, SelectorLookup lookup, Properties props){
		this.input=input;
		this.lookup=lookup;
		this.props=props;
	}
	
	public void calculate(){
		
		List<Customer> codedE = new ArrayList<Customer>();
		List<Customer> uncodedE = new ArrayList<Customer>();
		List<Customer> fleetE = new ArrayList<Customer>();
		List<Customer> clericalE = new ArrayList<Customer>(); 
		List<Customer> multiE = new ArrayList<Customer>();
		List<Customer> codedW = new ArrayList<Customer>();
		List<Customer> uncodedW = new ArrayList<Customer>();
		List<Customer> fleetW = new ArrayList<Customer>();
		List<Customer> clericalW = new ArrayList<Customer>(); 
		List<Customer> multiW = new ArrayList<Customer>();
		
		for(Customer customer : input){

			switch (customer.getBatchType()) {
				case "CODED":  
					if("E".equals(customer.getLang())){
						codedE.add(customer);
					}else{
						codedW.add(customer);
					};
				break;
				case "UNCODED": 
					if("E".equals(customer.getLang())){
						uncodedE.add(customer);
					}else{
						uncodedW.add(customer);
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
			}
		}
		
		LOGGER.info("Batch contains:\nCODED E{} W{}\nUNCODED E{} W{}\nFLEET E{} W{}\nCLERICAL E{} W{}\nMULTI E{} W{}",
				codedE.size(),codedW.size(), uncodedE.size(),uncodedW.size(),fleetE.size(),fleetW.size(),clericalE.size(),clericalW.size(),multiE.size(),multiW.size());
		int codedCount = 0;
		int uncodedCount = 0;
		Set<String> eFleetGroupsToFf = null;
		Set<String> eClericalGroupsToFf = null;
		Set<String> eMultiGroupsToFf = null;
		Set<String> wFleetGroupsToFf = null;
		Set<String> wClericalGroupsToFf = null;
		Set<String> wMultiGroupsToFf = null;
		boolean firstCustomer = true;
		Integer eCodedToFf = null;
		Integer wCodedToFf = null;
		Integer eUncodedToFf = null;
		Integer wUncodedToFf = null;
		Integer codedNumberToFf = null;
		Integer uncodedNumberToFf = null;
		
		for(Customer customer : input){
			if(firstCustomer && lookup.get(customer.getSelectorRef()) == null){
				LOGGER.fatal("The reference '{}' couldn't be found in lookup '{}' for customer with doc ref={}",customer.getSelectorRef(),lookup.getFile(),customer.getDocRef());
				System.exit(1);
				firstCustomer =false;
			}
			if( "CODED".equals(customer.getBatchType()) ){
				if( isNumeric(lookup.get(customer.getSelectorRef()).getCoded().trim()) ){
					
					if( "E".equals(customer.getLang()) ){
						if(eCodedToFf == null){
							eCodedToFf = (int) ( codedE.size() * ( (float)Integer.parseInt(lookup.get(customer.getSelectorRef()).getCoded().trim()) / 100 ) );
							codedNumberToFf = eCodedToFf;
							codedCount = 0;
							LOGGER.info("Size of english coded= {}, FF set to {}",codedE.size(),codedNumberToFf );
						}
					}else{
						if(wCodedToFf == null){
							wCodedToFf = (int) ( codedW.size() * ( (float)Integer.parseInt(lookup.get(customer.getSelectorRef()).getCoded().trim()) / 100 ) );
							codedNumberToFf = wCodedToFf;
							codedCount = 0;
							LOGGER.info("Size of welsh coded= {}, FF set to {}",codedW.size(),codedNumberToFf );
						}
					}
					
					if (codedCount < codedNumberToFf){
						customer.setSite("f");
						codedCount ++;
					}else{
						customer.setSite("m");
					}
				}else{
					if("M".equalsIgnoreCase(lookup.get(customer.getSelectorRef()).getCoded().trim()) ){
						customer.setSite("m");
					}else{
						customer.setSite("f");
					}
				}
			}
			if( "UNCODED".equals(customer.getBatchType()) ){
				if( isNumeric(lookup.get(customer.getSelectorRef()).getUncoded().trim()) ){
					
					int numberToFf = 0;
					if( "E".equals(customer.getLang()) ){
						if(eUncodedToFf == null){
							eUncodedToFf = (int) ( uncodedE.size() * ( (float)Integer.parseInt(lookup.get(customer.getSelectorRef()).getCoded().trim()) / 100 ) );
							uncodedNumberToFf = eUncodedToFf;
							uncodedCount = 0;
							LOGGER.info("Size of english uncoded= {}, FF set to {}",uncodedE.size(),uncodedNumberToFf );
						}
					}else{
						if(wUncodedToFf == null){
							wUncodedToFf = (int) ( uncodedW.size() * ( (float)Integer.parseInt(lookup.get(customer.getSelectorRef()).getCoded().trim()) / 100 ) );
							uncodedNumberToFf = wUncodedToFf;
							uncodedCount = 0;
							LOGGER.info("Size of welsh uncoded= {}, FF set to {}",uncodedW.size(),uncodedNumberToFf );
						}
					}
					
					if (uncodedCount < uncodedNumberToFf){
						customer.setSite("f");
						uncodedCount ++;
					}else{
						customer.setSite("m");
					}
				}else{
					if( "M".equalsIgnoreCase(lookup.get(customer.getSelectorRef()).getUncoded().trim()) ){
						customer.setSite("m");
					}else{
						customer.setSite("f");
					}
				}
			}
			if( "FLEET".equals(customer.getBatchType()) ){
				if( isNumeric(lookup.get(customer.getSelectorRef()).getFleet().trim()) ){
					if( "E".equals(customer.getLang()) ){
						if(eFleetGroupsToFf == null){
							int numberToFf = (int) ( fleetE.size() * ( (float)Integer.parseInt(lookup.get(customer.getSelectorRef()).getCoded().trim()) / 100 ) );
							eFleetGroupsToFf = calculateGroupLocation(fleetE, numberToFf);
						}
						if (eFleetGroupsToFf.contains(customer.getGroupId())){
							customer.setSite("f");
						}else{
							customer.setSite("m");
						}
					}else{
						if(wFleetGroupsToFf == null){
							int numberToFf = (int) ( fleetW.size() * ( (float)Integer.parseInt(lookup.get(customer.getSelectorRef()).getCoded().trim()) / 100 ) );
							wFleetGroupsToFf = calculateGroupLocation(fleetW, numberToFf);
						}
						if (wFleetGroupsToFf.contains(customer.getGroupId())){
							customer.setSite("f");
						}else{
							customer.setSite("m");
						}
					}
				}else{
					if( "M".equalsIgnoreCase(lookup.get(customer.getSelectorRef()).getFleet().trim()) ){
						customer.setSite("m");
					}else{
						customer.setSite("f");
					}
				}
			}
			if( "CLERICAL".equals(customer.getBatchType()) ){
				if( isNumeric(lookup.get(customer.getSelectorRef()).getClerical().trim()) ){
					if( "E".equals(customer.getLang()) ){
						if(eClericalGroupsToFf == null){
							int numberToFf = (int) ( clericalE.size() * ( (float)Integer.parseInt(lookup.get(customer.getSelectorRef()).getCoded().trim()) / 100 ) );
							eClericalGroupsToFf = calculateGroupLocation(clericalE, numberToFf);
						}
						if (eClericalGroupsToFf.contains(customer.getGroupId())){
							customer.setSite("f");
						}else{
							customer.setSite("m");
						}
					}else{
						if(wClericalGroupsToFf == null){
							int numberToFf = (int) ( clericalW.size() * ( (float)Integer.parseInt(lookup.get(customer.getSelectorRef()).getCoded().trim()) / 100 ) );
							wClericalGroupsToFf = calculateGroupLocation(fleetW, numberToFf);
						}
						if (wClericalGroupsToFf.contains(customer.getGroupId())){
							customer.setSite("f");
						}else{
							customer.setSite("m");
						}
					}
				}else{
					if( "M".equalsIgnoreCase(lookup.get(customer.getSelectorRef()).getClerical().trim()) ){
						customer.setSite("m");
					}else{
						customer.setSite("f");
					}
				}
			}
			if( "MULTI".equals(customer.getBatchType()) ){
				if( isNumeric(lookup.get(customer.getSelectorRef()).getMulti()) ){
					if( "E".equals(customer.getLang()) ){
						if(eMultiGroupsToFf == null){
							int numberToFf = (int) ( multiE.size() * ( (float)Integer.parseInt(lookup.get(customer.getSelectorRef()).getCoded().trim()) / 100 ) );
							eMultiGroupsToFf = calculateGroupLocation(multiE, numberToFf);
						}
						if (eMultiGroupsToFf.contains(customer.getGroupId())){
							customer.setSite("f");
						}else{
							customer.setSite("m");
						}
					}else{
						if(wMultiGroupsToFf == null){
							int numberToFf = (int) ( multiW.size() * ( (float)Integer.parseInt(lookup.get(customer.getSelectorRef()).getCoded().trim()) / 100 ) );
							wMultiGroupsToFf = calculateGroupLocation(multiW, numberToFf);
						}
						if (wMultiGroupsToFf.contains(customer.getGroupId())){
							customer.setSite("f");
						}else{
							customer.setSite("m");
						}
					}
				}else{
					if( "M".equalsIgnoreCase(lookup.get(customer.getSelectorRef()).getMulti().trim()) ){
						customer.setSite("m");
					}else{
						customer.setSite("f");
					}
				}
			}
			//LOGGER.info("Customer {} site now set to {}",customer.getDocRef(), customer.getSite());
			
		}
	}
	
	private boolean isNumeric(String s) {  
	    return s.matches("[-+]?\\d*\\.?\\d+");  
	}
	
	private Set<String> calculateGroupLocation(List<Customer> customers, int noToFf){
		LOGGER.info("Calculating split for {} customers, number to Ff={}",customers.size(),noToFf);
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
