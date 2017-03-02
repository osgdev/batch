package uk.gov.dvla.osg.batch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import uk.gov.dvla.osg.common.classes.Customer;
import uk.gov.dvla.osg.common.classes.EnvelopeLookup;
import uk.gov.dvla.osg.common.classes.InsertLookup;
import uk.gov.dvla.osg.common.classes.PostageConfiguration;
import uk.gov.dvla.osg.common.classes.ProductionConfiguration;
import uk.gov.dvla.osg.common.classes.RpdFileHandler;
import uk.gov.dvla.osg.common.classes.SelectorLookup;
import uk.gov.dvla.osg.common.classes.StationeryLookup;
import uk.gov.dvla.osg.ukmail.resources.CreateUkMailResources;

public class Main {
	
	private static final Logger LOGGER = LogManager.getLogger();
	private static final Properties CONFIG = new Properties();
	static ArrayList<Customer> customers;

	public static void main(String[] args) {
		LOGGER.info("Starting uk.gov.dvla.osg.batch.Main");
		
		if( args.length != 5 ){
			LOGGER.fatal("Incorrect number of args parsed '{}'. Args are 1.input file, 2.output file, 3.props file, 4.jobId, 5.Runno.",args.length);
			System.exit(1);
		}
		
		String input = args[0];
		String output = args[1];
		String propsFile = args[2];
		String jid = args[3];
		String runNo = args[4];
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
			/*//Define input csv
			FileReader in = new FileReader(input);
			CSVFormat inputFormat= CSVFormat.RFC4180.withFirstRecordAsHeader();
			
			//Define output csv
			Appendable out = new FileWriter(output);
			CSVFormat outputFormat = CSVFormat.RFC4180.withQuoteMode(QuoteMode.ALL);
			CSVPrinter printer = new CSVPrinter(out, outputFormat);
		
			//Get Headers from csv
			CSVParser csvFileParser = new CSVParser(in, inputFormat);
			Map<String, Integer> headers = csvFileParser.getHeaderMap();*/
			
			RpdFileHandler fh = new RpdFileHandler(input, output);
			
			List<String> heads = fh.getHeaders();
			/*for(Map.Entry<String,Integer> en : headers.entrySet()){
				heads.add(en.getKey());
			}*/

			//LOGGER.debug(heads);
			//reqFields is used to validate input, the Y signifies that the field should be present in the input file
			List<String> reqFields = new ArrayList<String>();
			String docRef = CONFIG.getProperty("documentReference");
			reqFields.add(docRef + ",documentReference,Y");
			String noOfPages =CONFIG.getProperty("noOfPagesField");
			reqFields.add(noOfPages + ",noOfPagesField,Y");
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
			String name1Field = CONFIG.getProperty("name1Field");
			reqFields.add(name1Field + ",name1Field,Y");
 			String name2Field = CONFIG.getProperty("name2Field");
			reqFields.add(name2Field + ",name2Field,Y");
			String add1Field = CONFIG.getProperty("address1Field");
			reqFields.add(add1Field + ",address1Field,Y");
			String add2Field = CONFIG.getProperty("address2Field");
			reqFields.add(add2Field + ",address2Field,Y");
			String add3Field = CONFIG.getProperty("address3Field");
			reqFields.add(add3Field + ",address3Field,Y");
			String add4Field = CONFIG.getProperty("address4Field");
			reqFields.add(add4Field + ",address4Field,Y");
			String add5Field = CONFIG.getProperty("address5Field");
			reqFields.add(add5Field + ",address5Field,Y");
			String pcField = CONFIG.getProperty("postCodeField");
			reqFields.add(pcField + ",postCodeField,Y");
			String dpsField = CONFIG.getProperty("dpsField");
			reqFields.add(dpsField + ",dpsField,Y");
			String insertLookup = CONFIG.getProperty("insertLookup");
			reqFields.add(insertLookup + ",insertLookup,N");
			String envelopeLookup = CONFIG.getProperty("envelopeLookup");
			reqFields.add(envelopeLookup + ",envelopeLookup,N");
			String stationeryLookup = CONFIG.getProperty("stationeryLookup");
			reqFields.add(stationeryLookup + ",stationeryLookup,N");
			String insertField = CONFIG.getProperty("insertField");
			reqFields.add(insertField + ",insertField,Y");
			String mmBarContent = CONFIG.getProperty("mailMarkBarcodeContent");
			reqFields.add(mmBarContent + ",mailMarkBarcodeContent,Y");
			String eogField = CONFIG.getProperty("eogField");
			reqFields.add(eogField + ",eogField,Y");
			String eotField = CONFIG.getProperty("eotField");
			reqFields.add(eotField + ",eotField,Y");
			String seqField = CONFIG.getProperty("childSequence");
			reqFields.add(seqField + ",childSequence,Y");
			String outEnv = CONFIG.getProperty("outerEnvelope");
			reqFields.add(outEnv + ",outerEnvelope,Y");
			String mailingProduct = CONFIG.getProperty("mailingProduct");
			reqFields.add(mailingProduct + ",mailingProduct,Y");
			
			
			
			
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
			
			fh.write(fh.getHeaders());
			
			ProductionConfiguration productionConfig = null;
			PostageConfiguration postageConfig = null;
			
			
			File f = new File(input);
            BufferedReader b = new BufferedReader(new FileReader(f));
            String readLine = b.readLine();
            //Read headers
            LOGGER.debug("Read line as header '{}'",readLine);

            HashMap<String,Integer> fileMap = fh.getMapping();
            
            boolean firstCustomer = true;
			Map<String,Integer> presLookup = new HashMap<String,Integer>();
			String presConfig ="";
			String batchComparator = "";
			String msc = "";
			int custCounter=0;
			
            while ((readLine = b.readLine()) != null) {
            	String[] split = readLine.split("\\t",-1);
            	
            	
            	
            	if(firstCustomer){
					//Create Map of presentation priorities
					if (lookup.get(split[fileMap.get(selectorRef)]) == null){
						LOGGER.fatal("Selector '{}' not found in lookup '{}'",selectorRef,lookupFile);
						System.exit(1);
					}
					presConfig = presentationPriorityConfigPath + lookup.get(split[fileMap.get(selectorRef)]).getPresentationConfig() + presentationPriorityFileSuffix;
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
					
					productionConfig = new ProductionConfiguration(productionConfigPath + lookup.get(split[fileMap.get(selectorRef)]).getProductionConfig() + productionFileSuffix );
					postageConfig = new PostageConfiguration(postageConfigPath + lookup.get(split[fileMap.get(selectorRef)]).getPostageConfig() + postageFileSuffix );
					firstCustomer=false;
				}
				
				msc = split[fileMap.get(mscField)];
				
				Customer customer = new Customer(custCounter,
						split[fileMap.get(docRef)],
						split[fileMap.get(sortField)],
						split[fileMap.get(selectorRef)],
						split[fileMap.get(lang)],
						split[fileMap.get(stat)],
						split[fileMap.get(batchType)],
						split[fileMap.get(subBatch)],
						split[fileMap.get(fleetNo)],
						split[fileMap.get(groupId)],
						split[fileMap.get(paperSize)],
						msc);
				
				customer.setName1(split[fileMap.get(name1Field)]);
				customer.setName2(split[fileMap.get(name2Field)]);
				customer.setAdd1(split[fileMap.get(add1Field)]);
				customer.setAdd2(split[fileMap.get(add2Field)]);
				customer.setAdd3(split[fileMap.get(add3Field)]);
				customer.setAdd4(split[fileMap.get(add4Field)]);
				customer.setAdd5(split[fileMap.get(add5Field)]);
				customer.setPostcode(split[fileMap.get(pcField)]);
				customer.setDps(split[fileMap.get(dpsField)]);
				customer.setInsertRef(split[fileMap.get(insertField)]);
				
				
				if( split[fileMap.get(subBatch)] == null || split[fileMap.get(subBatch)].isEmpty() ){
					batchComparator = split[fileMap.get(batchType)];
				}else{
					batchComparator = split[fileMap.get(batchType)] + "_" + split[fileMap.get(subBatch)];
				}
				if(presLookup.get(batchComparator) == null){
					LOGGER.error("Batch type '{}' not found in presentation config '{}' setting priotity to 999",batchComparator,presConfig);
					customer.setPresentationPriority(999);
				}else{
					customer.setPresentationPriority(presLookup.get(batchComparator));
				}
				
				customer.setNoOfPages(Integer.parseInt(split[fileMap.get(noOfPages)]));
				
				customers.add(customer);
				custCounter++;
            }
            b.close();
            	
			LOGGER.info("Created {} customers",customers.size());
			
			try{
				//SORT HERE
				Collections.sort(customers, new CustomerComparator());
			}catch (Exception e){
				LOGGER.fatal("Error when sorting: '{}'",e.getMessage());
				System.exit(1);
			}
			
			//Check compliance including:
			//		MSC groups of under 25
			//		Compliance level over 83%
			CheckCompliance cc = new CheckCompliance(customers, productionConfig, postageConfig);
			
			//Calculate how we are actually going to send this run, UNSORTED, SORTED via MM, SORTED via OCR
			String actualMailProduct="";
			if( "UNSORTED".equalsIgnoreCase(productionConfig.getMailsortProduct()) || cc.getTotalMailsortCount() < Integer.parseInt(productionConfig.getMinimumMailsort()) ){
				actualMailProduct="UNSORTED";
				for(Customer customer : customers){
					//SET FINAL ENVELOPE
					if("E".equalsIgnoreCase(customer.getLang()) ){
						customer.setEnvelope(productionConfig.getEnvelopeEnglishUnsorted());
					}else{
						customer.setEnvelope(productionConfig.getEnvelopeWelshUnsorted());
					}
					//CHANGE BATCH TYPE TO UNSORTED FOR ALL SORTED
					if("SORTED".equalsIgnoreCase(customer.getBatchType()) ){
						customer.setBatchType("UNSORTED");
					}
					customer.setProduct(actualMailProduct);
				}
			} else if( "OCR".equalsIgnoreCase(productionConfig.getMailsortProduct()) ){
				actualMailProduct="OCR";
				for(Customer customer : customers){
					//SET FINAL ENVELOPE
					if("E".equalsIgnoreCase(customer.getLang()) ){
						customer.setEnvelope(productionConfig.getEnvelopeEnglishOcr());
					}else{
						customer.setEnvelope(productionConfig.getEnvelopeWelshOcr());
					}
					customer.setProduct(actualMailProduct);
				}
			} else if( "MM".equalsIgnoreCase(productionConfig.getMailsortProduct()) ){
				if( cc.getDpsAccuracy() < postageConfig.getUkmMinimumCompliance() ){
					actualMailProduct="OCR";
				} else {
					actualMailProduct="MM";
					//Apply default DPS
					for(Customer customer : customers){
						//SET DEFAULT DPS IF APPLICABLE
						if( postageConfig.getUkmBatchTypes().contains(customer.getBatchType()) && 
								(customer.getDps().isEmpty() || customer.getDps() == null) ){
							customer.setDps("9Z");
						}
						//SET FINAL ENVELOPE
						if("E".equalsIgnoreCase(customer.getLang()) ){
							customer.setEnvelope(productionConfig.getEnvelopeEnglishMm());
						}else{
							customer.setEnvelope(productionConfig.getEnvelopeWelshMm());
						}
						customer.setProduct(actualMailProduct);
					}
				}
			} else {
				LOGGER.fatal("Failed to determine mailing product from {}. Mailsort product set to '{}'",productionConfig.getFilename(), productionConfig.getMailsortProduct());
				System.exit(1);
			}
			LOGGER.info("Run will be sent via {} product.",actualMailProduct);
			
			CalculateLocation cl = new CalculateLocation(customers, lookup, productionConfig);
			cl.calculate();
		
			try{
				//SORT HERE
				Collections.sort(customers, new CustomerComparatorWithLocation());
			}catch (Exception e){
				LOGGER.fatal("Error when sorting: '{}'",e.getMessage());
				e.printStackTrace();
				System.exit(1);
			}
			
			StationeryLookup sl = new StationeryLookup(stationeryLookup);
			EnvelopeLookup el = new EnvelopeLookup(envelopeLookup);
			InsertLookup il = new InsertLookup(insertLookup);
			
			CalculateEndOfGroups eogs = new CalculateEndOfGroups(customers, productionConfig);
			eogs.calculate();
			CalculateWeightsAndSizes cwas = new CalculateWeightsAndSizes(customers, il, sl, el, productionConfig);
			cwas.calculate();

			//Sets jobId, batchSequence and Sequence
			BatchEngine be = new BatchEngine(jid, customers, productionConfig, postageConfig, actualMailProduct);
			be.batch();
			
			
			CreateUkMailResources ukm = new CreateUkMailResources(customers, postageConfig, productionConfig, cc.getDpsAccuracy(), runNo,actualMailProduct );
			
			
			try{
				//SORT BACK TO ORIGINAL ORDER HERE
				Collections.sort(customers, new CustomerComparatorOriginalOrder());
			}catch (Exception e){
				LOGGER.fatal("Error when sorting: '{}'",e.getMessage());
				System.exit(1);
			}
			
			
			
			BufferedReader bu = new BufferedReader(new FileReader(f));
			readLine = bu.readLine();
			List<String> list = new ArrayList<String>();
			
			int i = 0;
			int jidIdx = fileMap.get(jidField);
			int siteIdx = fileMap.get(site);
			int eogIdx = fileMap.get(eogField);
			int eotIdx = fileMap.get(eotField);
			int mailContentIdx = fileMap.get(mmBarContent);
			int seqFieldIdx = fileMap.get(seqField);
			int outEnvIdx = fileMap.get(outEnv);
			int mailingProductIdx = fileMap.get(mailingProduct);
			
			
			while ((readLine = bu.readLine()) != null) {
				String[] split = readLine.split("\\t",-1);
				list.clear();
				for( int x = 0; x < split.length; x ++ ){
					if( x == jidIdx ){
						if( customers.get(i).getJid() != null){
							list.add(customers.get(i).getJid());
						} else{
							list.add("");
						}
					} else if( x == siteIdx ){
						if( customers.get(i).getSite() != null){
							list.add("" + customers.get(i).getSite());
						}else{
							list.add("");
						}
					} else if( x ==  eogIdx){
						if( customers.get(i).getEog() != null){
							list.add("" + customers.get(i).getEog());
						}else{
							list.add("");
						}
					}else if( x == eotIdx ){
						if( customers.get(i).getEot() != null){
							list.add("" + customers.get(i).getEot());
						}else{
							list.add("");
						}
					}else if( x == mailContentIdx ){
						if( customers.get(i).getMmBarcodeContent() != null){
							list.add("" + customers.get(i).getMmBarcodeContent());
						}else{
							list.add("");
						}
					}else if( x == seqFieldIdx ){
						list.add("" + customers.get(i).getSequence());
					}else if( x == outEnvIdx ){
						list.add(customers.get(i).getEnvelope());
					}else if( x == mailingProductIdx ){
						list.add(customers.get(i).getProduct());
					}else {
						list.add(split[x]);
					}
				}
				fh.write(list);
				i++;
			}
            fh.closeFile();
			
			
			/*for (Customer customer : customers){
				//printer.printRecord((Object[])customer.print());
				printer.printRecord(customer);
			}*/
			//csvFileParser.close();
			//printer.close();
			
		} catch (IOException e) {
			LOGGER.fatal(e.getMessage());
			System.exit(1);
		}
	}
}
