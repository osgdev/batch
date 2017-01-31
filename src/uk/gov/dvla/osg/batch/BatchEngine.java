package uk.gov.dvla.osg.batch;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BatchEngine {
	private static final Logger LOGGER = LogManager.getLogger(Main.class.getName());
	private List<Customer> input;
	private SelectorLookup lookup;
	private Properties props;
	private String parentJid;
	
	public BatchEngine(String parentJid, List<Customer> input, SelectorLookup lookup, Properties props){
		this.parentJid=parentJid;
		this.input=input;
		this.lookup=lookup;
		this.props=props;
	}
	
	public void batch(){
		
		
		int j =1;
		int k =1;
		boolean firstCustomer = true;
		Customer prev = null;
		int batchMax = 0;
		int count =0;
		for (Customer customer : input){
			if(firstCustomer){
				prev = customer;
				batchMax = lookup.get(customer.getSelectorRef()).getBatchMax();
				firstCustomer=false;
			}
			if( (prev.getLang().equals(customer.getLang()) ) && 
					(prev.getBatchType().equals(customer.getBatchType())) &&
					(prev.getSubBatch().equals(customer.getSubBatch())) &&
					(prev.getSite().equals(customer.getSite())) &&
					(count < batchMax) ){
				customer.setJid(parentJid + "." + j);
			}else{
				j ++;
				k=1;
				count = 0;
				customer.setJid(parentJid + "." + j);
			}
			/*LOGGER.info("customer lang='{}' prev='{}'     bt='{}' prev='{}'    sbt='{}' prev='{}'   site='{}' prev='{}'",
					customer.getLang(),prev.getLang(),customer.getBatchType(),prev.getBatchType(),
					customer.getSubBatch(), customer.getSubBatch(),customer.getSite(), prev.getSite());*/
			prev = customer;
			customer.setBatchSequence(j);
			customer.setSequence(k);
			k++;
			count ++;
		}
		
	}
	
}
