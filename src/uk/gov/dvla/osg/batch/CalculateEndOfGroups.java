package uk.gov.dvla.osg.batch;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import uk.gov.dvla.osg.common.classes.Customer;
import uk.gov.dvla.osg.common.classes.ProductionConfiguration;

public class CalculateEndOfGroups {
	private static final Logger LOGGER = LogManager.getLogger();
	private List<Customer> input;
	private ProductionConfiguration pc;
	
	public CalculateEndOfGroups(List<Customer> input, ProductionConfiguration pc){
		LOGGER.info("Starting CalculateEndOfGroups");
		this.input=input;
		this.pc=pc;
	}
	
	public void calculate(){
		Customer cus, next;
		int pageCount=0;
		int maxPages=0;
		int j=0;
		
		for(int i = 0; i < input.size();i++){
			if( i+1 < input.size() ){
				j=i+1;
				cus = input.get(i);
				next = input.get(j);
				maxPages = getMaxPages(cus);
				if( !(cus.getGroupId() == null) && !(cus.getGroupId().isEmpty()) ){
					
					if( cus.getGroupId().equalsIgnoreCase(next.getGroupId()) ){
						pageCount = pageCount + cus.getNoOfPages();
						LOGGER.debug("Group ID set, page count={}, max pages={}, this plus next={}",pageCount,maxPages,(pageCount + next.getNoOfPages()));
						if( (pageCount + next.getNoOfPages()) > maxPages ){
							cus.setEog("X");
							pageCount=0;
						}
					}else{
						cus.setEog("X");
						pageCount=0;
					}
				}else{
					LOGGER.debug("No group ID set, setting EOG to X");
					cus.setEog("X");
					pageCount=0;
				}
			}else{
				//Last customer
				LOGGER.info("Processing last customer");
				cus = input.get(i);
				cus.setEog("X");
			}
		}
		
	}

	private int getMaxPages(Customer cus) {
		int result = 0;
		if("E".equalsIgnoreCase( cus.getLang() ) ){
			if("CLERICAL".equalsIgnoreCase(cus.getBatchType())){
				result = pc.getGroupMaxEnglishClerical();
			}else if ("MULTI".equalsIgnoreCase(cus.getBatchType())){
				result = pc.getGroupMaxEnglishMulti();
			}else if ("FLEET".equalsIgnoreCase(cus.getBatchType())){
				result = pc.getGroupMaxEnglishFleet();
			}
		}else{
			if("CLERICAL".equalsIgnoreCase(cus.getBatchType())){
				result = pc.getGroupMaxWelshClerical();
			}else if ("MULTI".equalsIgnoreCase(cus.getBatchType())){
				result = pc.getGroupMaxWelshMulti();
			}else if ("FLEET".equalsIgnoreCase(cus.getBatchType())){
				result = pc.getGroupMaxWelshFleet();
			}
		}
		
		return result;
	}
}
