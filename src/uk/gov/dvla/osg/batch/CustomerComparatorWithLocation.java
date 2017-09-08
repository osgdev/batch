package uk.gov.dvla.osg.batch;

import java.util.Comparator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import uk.gov.dvla.osg.common.classes.Customer;

public class CustomerComparatorWithLocation implements Comparator<Customer>{
	private static final Logger LOGGER = LogManager.getLogger();
	@Override
	public int compare(Customer o1, Customer o2) {
		/*
		 * SORT ORDER IS:
		 * LOCATION
		 * LANGUAGE
		 * STATIONERY
		 * PRESENTATION_ORDER
		 * SUB_BATCH
		 * SORT_FIELD
		 * FLEET_NO
		 * MSC
		 * GRP_ID
		 */

		// First by LOCATION - stop if this gives a result.
		int locationResult = o1.getSite().compareTo(o2.getSite());
		
        if (locationResult != 0){
        	return locationResult;
       	}
		
        // Next by LANGUAGE - stop if this gives a result.
        int langResult = o1.getLang().compareTo(o2.getLang());
        //LOGGER.debug("Comparing '{}' with '{}' result '{}' for refs '{}' & '{}'",o1.getLang(),o2.getLang(),langResult,o1.getDocRef(),o2.getDocRef());
        if (langResult != 0){
            return langResult;
        }
        
     // Next by STATIONERY - stop if this gives a result.
        int statResult = o1.getStationery().compareTo(o2.getStationery());
        //LOGGER.debug("Comparing '{}' with '{}' result '{}' for refs '{}' & '{}'",o1.getLang(),o2.getLang(),langResult,o1.getDocRef(),o2.getDocRef());
        if (statResult != 0){
            return statResult;
        }
        
        // Next by PRESENTATION_ORDER - stop if this gives a result.
        int presResult = o1.getPresentationPriority().compareTo(o2.getPresentationPriority());
        //LOGGER.debug("Comparing '{}' with '{}' result '{}' for ref '{}' & '{}'",o1.getPresentationPriority(),o2.getPresentationPriority(),presResult,o1.getDocRef(),o2.getDocRef());
        if (presResult != 0){
            return presResult;
        }
		
		// Next by SUB_BATCH - stop if this gives a result.
        int subBatchResult = o1.getSubBatch().compareTo(o2.getSubBatch());
        //LOGGER.debug("Comparing '{}' with '{}' result '{}' for ref '{}' & '{}'",o1.getSubBatch(),o2.getSubBatch(),subBatchResult,o1.getDocRef(),o2.getDocRef());
        if (subBatchResult != 0){
            return subBatchResult;
        }
        
        // Next by SORT_FIELD
        int sortFieldResult = o1.getSortField().compareTo(o2.getSortField());
        //LOGGER.debug("Comparing '{}' with '{}' result '{}' for ref '{}' & '{}'",o1.getSortField(),o2.getSortField(),sortFieldResult,o1.getDocRef(),o2.getDocRef());
        if( sortFieldResult !=0 ){
        	return sortFieldResult;
        }

        // Next by FLEET_NO
        int fleetResult = o1.getFleetNo().compareTo(o2.getFleetNo());
        //LOGGER.debug("Comparing '{}' with '{}' result '{}' for ref '{}' & '{}'",o1.getFleetNo(),o2.getFleetNo(),fleetResult,o1.getDocRef(),o2.getDocRef());
        if (fleetResult != 0){
            return fleetResult;
        }
        
        // Next by MSC
        int mscResult = o1.getMsc().compareTo(o2.getMsc());
        //LOGGER.debug("Comparing '{}' with '{}' result '{}' for ref '{}' & '{}'",o1.getMsc(),o2.getMsc(),mscResult,o1.getDocRef(),o2.getDocRef());
        if (mscResult != 0){
            return mscResult;
        }

        
        // Next by Post Code
        int pcResult = o1.getPostcode().compareTo(o2.getPostcode());
        //LOGGER.debug("Comparing '{}' with '{}' result '{}' for ref '{}' & '{}'", o1.getMsc(),o2.getMsc(),mscResult,o1.getDocRef(),o2.getDocRef());
        if (pcResult != 0){
            return pcResult;
        }
        
        // Finally by GRP_ID
        return o1.getGroupId().compareTo(o2.getGroupId());
         
	}

}
