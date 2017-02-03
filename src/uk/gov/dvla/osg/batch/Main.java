package uk.gov.dvla.osg.batch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {
	
	private static final Logger LOGGER = LogManager.getLogger(Main.class.getName());
	private static final Properties CONFIG = new Properties();
	static List<Customer> customers;

	public static void main(String[] args) {
		LOGGER.info("Starting uk.gov.dvla.osg.batch.Main");
		
		if( args.length != 4 ){
			LOGGER.fatal("Incorrect number of args parsed '{}'",args.length);
			System.exit(1);
		}
		
		String input = args[0];
		String output = args[1];
		String propsFile = args[2];
		String jid = args[3];
		SelectorLookup lookup = null;
		customers = new ArrayList<Customer>();
		
		if( !(new File(args[0]).exists()) ){
			LOGGER.fatal("File '{}' doesn't exist",args[0]);
			System.exit(1);
		}
		
		if(new File(propsFile).exists()){
			try {
				CONFIG.load(new FileInputStream(propsFile));
			} catch (IOException e) {
				LOGGER.fatal("Log file didn't load: '{}'",e.getMessage());
				System.exit(1);
			}
		}else{
			LOGGER.fatal("Log file: '{}' doesn't exist",propsFile);
			System.exit(1);
		}
		String lookupFile=CONFIG.getProperty("lookupFile");
		if( new File(lookupFile).exists()){
			LOGGER.debug("lookupfile='{}' Config='{}",lookupFile,CONFIG.size());
			lookup = new SelectorLookup(lookupFile, CONFIG);
		}else{
			LOGGER.fatal("Lookup file='{}' doesn't exist",lookupFile);
			System.exit(1);
		}
		
		try {
			//Define input csv
			FileReader in = new FileReader(input);
			CSVFormat inputFormat= CSVFormat.RFC4180.withFirstRecordAsHeader();
			
			//Define output csv
			Appendable out = new FileWriter(output);
			CSVFormat outputFormat = CSVFormat.RFC4180.withQuoteMode(QuoteMode.ALL);
			CSVPrinter printer = new CSVPrinter(out, outputFormat);
		
			//Get Headers from csv
			CSVParser csvFileParser = new CSVParser(in, inputFormat);
			Map<String, Integer> headers = csvFileParser.getHeaderMap();
			
			List<String> heads = new ArrayList<String>();
			for(Map.Entry<String,Integer> en : headers.entrySet()){
				heads.add(en.getKey());
			}

			LOGGER.debug(heads);
			List<String> reqFields = new ArrayList<String>();
			String docRef = CONFIG.getProperty("documentReference");
			reqFields.add(docRef + ",documentReference,Y");
			String lang = CONFIG.getProperty("languageFieldName");
			reqFields.add(lang + ",languageFieldName,Y");
			String stat = CONFIG.getProperty("stationeryFieldName");
			reqFields.add(stat + ",stationeryFieldName,Y");
			String batchType = CONFIG.getProperty("batchTypeFieldName");
			reqFields.add(batchType + ",batchTypeFieldName,Y");
			String subBatch = CONFIG.getProperty("subBatchTypeFieldName");
			reqFields.add(subBatch + ",subBatchTypeFieldName,Y");
			String selectorRef = CONFIG.getProperty("lookupReferenceFieldName");
			reqFields.add(selectorRef + ",lookupReferenceFieldName,Y");
			String site = CONFIG.getProperty("siteFieldName");
			reqFields.add(site + ",siteFieldName,N");
			String fleetNo = CONFIG.getProperty("fleetNoFieldName");
			reqFields.add(fleetNo + ",fleetNoFieldName,Y");
			String groupId = CONFIG.getProperty("groupIdFieldName");
			reqFields.add(groupId + ",groupIdFieldName,Y");
			String paperSize = CONFIG.getProperty("paperSizeFieldName");
			reqFields.add(paperSize + ",paperSizeFieldName,Y");
			String jidField = CONFIG.getProperty("jobIdFieldName");
			reqFields.add(jidField + ",jobIdFieldName,N");
			String mscField = CONFIG.getProperty("mscFieldName");
			reqFields.add(mscField + ",mscFieldName,Y");
			String presentationPriorityConfigPath = CONFIG.getProperty("presentationPriorityConfigPath");
			reqFields.add(presentationPriorityConfigPath + ",presentationPriorityConfigPath,N");
			String presentationPriorityFileSuffix = CONFIG.getProperty("presentationPriorityFileSuffix");
			reqFields.add(presentationPriorityFileSuffix + ",presentationPriorityFileSuffix,N");
			String productionConfigPath = CONFIG.getProperty("productionConfigPath");
			reqFields.add(productionConfigPath + ",productionConfigPath,N");
			String productionFileSuffix = CONFIG.getProperty("productionFileSuffix");
			reqFields.add(productionFileSuffix + ",productionFileSuffix,N");
			String postageConfigPath = CONFIG.getProperty("postageConfigPath");
			reqFields.add(postageConfigPath + ",postageConfigPath,N");
			String postageFileSuffix = CONFIG.getProperty("postageFileSuffix");
			reqFields.add(postageFileSuffix + ",postageFileSuffix,N");
			String sortField = CONFIG.getProperty("sortField");
			reqFields.add(sortField + ",sortField,Y");
			
			for(String str : reqFields){
				String[] split = str.split(",");
				if ( "null".equals(split[0])){
					LOGGER.fatal("Field '{}' not in properties file {}.",split[1],propsFile);
					System.exit(1);
				}else{
					if( !(heads.contains(split[0])) && "Y".equals(split[2]) ){
						LOGGER.fatal("Field '{}' not found in input file {}.",split[1],input);
						System.exit(1);
					}
				}
			}
			
			printer.printRecord(docRef,site,jidField);
			
			ProductionConfiguration pc = null;
			
			Iterable<CSVRecord> records = csvFileParser.getRecords();
			boolean firstCustomer = true;
			Map<String,Integer> presLookup = new HashMap<String,Integer>();
			String presConfig ="";
			for (CSVRecord record : records) {
				if(firstCustomer){
					//Create Map of presentation priorities
					presConfig = presentationPriorityConfigPath + lookup.get(record.get(selectorRef)).getPresentationConfig() + presentationPriorityFileSuffix;
					if( !(new File(presConfig).exists()) ){
						LOGGER.fatal("Lookup file='{}' doesn't exist",presConfig);
						System.exit(1);
					}
					BufferedReader br = new BufferedReader(new FileReader(presConfig));  
					String line = null; 
					int k = 0;
					while ((line = br.readLine()) != null){
						presLookup.put(line.trim(), k);
						k++;
					}
					LOGGER.info("Presentation priority map '{}' contains {} values",presConfig, presLookup.size());
					
					pc = new ProductionConfiguration(productionConfigPath + lookup.get(record.get(selectorRef)).getProductionConfig() + productionFileSuffix );
					
					firstCustomer=false;
				}
				Customer customer = new Customer(
						record.get(docRef),
						record.get(sortField),
						record.get(selectorRef),
						record.get(lang),
						record.get(stat),
						record.get(batchType),
						record.get(subBatch),
						record.get(fleetNo),
						record.get(groupId),
						record.get(paperSize),
						record.get(mscField));
				
				if(presLookup.get(record.get(batchType)) == null){
					LOGGER.error("Batch type '{}' not found in presentation config '{}' setting priotity to 999",record.get(batchType),presConfig);
					customer.setPresentationPriority(999);
				}else{
					customer.setPresentationPriority(presLookup.get(record.get(batchType)));
				}
				
				customers.add(customer);
				//LOGGER.info("Created customer {}",customer);
			}
			LOGGER.info("Created {} customers",customers.size());
			
			

			try{
				//SORT HERE
				Collections.sort(customers, new CustomerComparator());
			}catch (Exception e){
				LOGGER.fatal("Error when sorting: '{}'",e.getMessage());
				System.exit(1);
			}
			
			LOGGER.debug("Pre-sort succesful");
			CalculateLocation cl = new CalculateLocation(customers, lookup, pc);
			cl.calculate();
		
			
			
			
			
			try{
				//SORT HERE
				Collections.sort(customers, new CustomerComparatorWithLocation());
			}catch (Exception e){
				LOGGER.fatal("Error when sorting: '{}'",e.getMessage());
				e.printStackTrace();
				System.exit(1);
			}
			
			/*for (Customer customer : customers){
				//printer.printRecord((Object[])customer.print());
				printer.printRecord(customer);
			}
			System.exit(0);*/
			
			BatchEngine be = new BatchEngine(jid, customers, lookup, pc);
			be.batch();
			for (Customer customer : customers){
				printer.printRecord((Object[])customer.print());
				//printer.printRecord(customer);
			}
			csvFileParser.close();
			printer.close();
			
		} catch (IOException e) {
			LOGGER.fatal(e.getMessage());
			System.exit(1);
		}
	}
}
