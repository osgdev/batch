package uk.gov.dvla.osg.batch;

import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import uk.gov.dvla.osg.common.classes.Customer;
import uk.gov.dvla.osg.common.classes.EnvelopeLookup;
import uk.gov.dvla.osg.common.classes.InsertLookup;
import uk.gov.dvla.osg.common.classes.ProductionConfiguration;
import uk.gov.dvla.osg.common.classes.StationeryLookup;

public class CalculateWeightsAndSizes {
	private static final Logger LOGGER = LogManager.getLogger();
	InsertLookup insertLookup = null;
	StationeryLookup stationeryLookup=null;
	EnvelopeLookup envelopeLookup=null;
	ProductionConfiguration pc=null;
	ArrayList<Customer> customers;
	
	public CalculateWeightsAndSizes(ArrayList<Customer> customers, 
			InsertLookup insertLookup,
			StationeryLookup stationeryLookup,
			EnvelopeLookup envelopeLookup,
			ProductionConfiguration pc){
		this.envelopeLookup=envelopeLookup;
		this.stationeryLookup=stationeryLookup;
		this.insertLookup=insertLookup;
		this.customers=customers;
		this.pc=pc;
		calculate();
	}
	
	private void calculate() {
		//INSERTS, ENVELOPE, PAPER all in mm
		float insertSize = 0;
		float insertWeight = 0;
		float paperSize = 0;
		float paperWeight = 0;
		float envelopeSize = 0;
		float envelopeWeight = 0;
		float totalSize = 0;
		float totalWeight = 0;
		int pageInGroupCount = 0;
		ArrayList<Customer> group = new ArrayList<Customer>();
		
		for(Customer cus : customers){
			try{
					
				if(!( cus.getInsertRef().isEmpty() )){
					insertSize=insertLookup.getLookup().get(cus.getInsertRef()).getThickness();
					insertWeight=insertLookup.getLookup().get(cus.getInsertRef()).getWeight();
				}else{
					insertSize=0;
					insertWeight=0;
				}
				envelopeSize = envelopeLookup.getLookup().get(pc.getEnvelopeType()).getThickness();
				envelopeWeight = envelopeLookup.getLookup().get(pc.getEnvelopeType()).getWeight();
				
				paperSize = stationeryLookup.getLookup().get(cus.getStationery()).getThickness() * envelopeLookup.getLookup().get(pc.getEnvelopeType()).getFoldMultiplier();
				paperWeight = stationeryLookup.getLookup().get(cus.getStationery()).getWeight() * cus.getNoOfPages();
			} catch (NullPointerException e){
				LOGGER.fatal("Envelope, Insert or Stationery lookup failed: '{}'", e.getMessage());
				System.exit(1);
			}
			
			if( "X".equalsIgnoreCase(cus.getEog()) ){
				totalSize = insertSize + paperSize + envelopeSize;
				totalWeight = insertWeight + paperWeight + envelopeWeight;
			} else {
				totalSize= paperSize;
				totalWeight=paperWeight;
			}
			
			//LOGGER.debug("Customer with doc ref '{}' size set to {}, weight set to {}",cus.getDocRef(),totalSize, totalWeight);
			cus.setWeight(totalWeight);
			cus.setSize(totalSize);
			
			//Calculate total pages in group
			pageInGroupCount = pageInGroupCount + cus.getNoOfPages();
			group.add(cus);
			if( "X".equalsIgnoreCase(cus.getEog()) ){
				
				for(Customer customer : group){
					customer.setTotalPagesInGroup(pageInGroupCount);
					//LOGGER.debug("Customer '{}' total pages set to {}",customer.getDocRef(),pageInGroupCount);
				}
				pageInGroupCount=0;
				group.clear();
			}
			
		}
		
	}
	
}
