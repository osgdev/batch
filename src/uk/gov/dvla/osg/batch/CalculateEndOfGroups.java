package uk.gov.dvla.osg.batch;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import uk.gov.dvla.osg.common.classes.Customer;

public class CalculateEndOfGroups {
	private static final Logger LOGGER = LogManager.getLogger(Main.class.getName());
	private List<Customer> input;
	
	public CalculateEndOfGroups(List<Customer> input){
		LOGGER.info("Starting CalculateEndOfGroups");
		this.input=input;
	}
	
	public void calculate(){
		Customer cus, next;
		int pageCount=0;
		int maxPages=6;
		int j=0;
		
		for(int i = 0; i < input.size();i++){
			if( i+1 < input.size() ){
				j=i+1;
				cus = input.get(i);
				next = input.get(j);
				if( !(cus.getGroupId() == null) && !(cus.getGroupId().isEmpty()) ){
					if( cus.getGroupId().equalsIgnoreCase(next.getGroupId()) ){
						pageCount = pageCount + cus.getNoOfPages();
						if( (pageCount + next.getNoOfPages()) > maxPages ){
							cus.setEog("X");
							pageCount=0;
						}
					}else{
						cus.setEog("X");
						pageCount=0;
					}
				}else{
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
}
