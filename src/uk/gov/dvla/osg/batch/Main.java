package uk.gov.dvla.osg.batch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import uk.gov.dvla.osg.common.classes.Customer;
import uk.gov.dvla.osg.common.classes.EnvelopeLookup;
import uk.gov.dvla.osg.common.classes.InsertLookup;
import uk.gov.dvla.osg.common.classes.PostageConfiguration;
import uk.gov.dvla.osg.common.classes.ProductionConfiguration;
import uk.gov.dvla.osg.common.classes.DocPropField;
import uk.gov.dvla.osg.common.classes.RpdFileHandler;
import uk.gov.dvla.osg.common.classes.SelectorLookup;
import uk.gov.dvla.osg.common.classes.StationeryLookup;
import uk.gov.dvla.osg.ukmail.resources.CreateUkMailResources;

public class Main {
	
	private static final Logger LOGGER = LogManager.getLogger();
	private static Properties CONFIG;
	private static final int EXPECTED_NO_OF_ARGS = 6;
	private static SelectorLookup lookup;
	static ArrayList<Customer> customers;
	static List<String> headerRecords;
	private static RpdFileHandler fh;
	static ProductionConfiguration productionConfig;
	static PostageConfiguration postageConfig;
	static HashMap<String,Integer> fileMap;
	static String actualMailProduct;
	static Map<String,Integer> presLookup;
	
	static StationeryLookup sl;
	static EnvelopeLookup el;
	static InsertLookup il;
	
	//Argument Strings
	static String input, output, propsFile, jid, runNo, lookupFile, parentJid;

	//Properties strings
	static String docRef, noOfPages, lang, stat, batchType, subBatch, selectorRef, site, fleetNo, groupId,
	paperSize, jidField, mscField, presentationPriorityConfigPath, presentationPriorityFileSuffix,
	productionConfigPath, productionFileSuffix, postageConfigPath, postageFileSuffix, sortField,
	name1Field, name2Field, add1Field, add2Field, add3Field, add4Field, add5Field, pcField,
	dpsField, insertLookup, envelopeLookup, stationeryLookup, insertField, mmBarContent, eogField,
	eotField, seqField, outEnv, mailingProduct, totalNumberOfPagesInGroupField, insertHopperCodeField, 
	mmCustomerContent, tenDigitJid, tenDigitJobIdIncrementValue;
	
	

	public static void main(String[] args) {
		LOGGER.info("Starting uk.gov.dvla.osg.batch.Main");
		validateNumberOfArgs(args);
		assignArgs(args);
		validateInputs();
		loadPropertiesFile();
		loadSelectorLookupFile();
		fh = new RpdFileHandler(input, output);
		headerRecords = fh.getHeaders();
		assignPropsFromPropsFile();
		sl = new StationeryLookup(stationeryLookup);
		el = new EnvelopeLookup(envelopeLookup);
		il = new InsertLookup(insertLookup);
		
		ensureRequiredPropsAreSet(headerRecords);
		generateCustomersFromInputFile();
		sortCustomers(customers, new CustomerComparator());
		CalculateLocation cl = new CalculateLocation(customers, lookup, productionConfig);
		sortCustomers(customers, new CustomerComparatorWithLocation());
		CalculateEndOfGroups eogs = new CalculateEndOfGroups(customers, productionConfig);
		CheckCompliance cc = new CheckCompliance(customers, productionConfig, postageConfig, presLookup);
		sortCustomers(customers, new CustomerComparatorWithLocation());
		calculateActualMailProduct(cc);
		sortCustomers(customers, new CustomerComparatorWithLocation());
		CalculateWeightsAndSizes cwas = new CalculateWeightsAndSizes(customers, il, sl, el, productionConfig);
		BatchEngine be = new BatchEngine(jid, customers, productionConfig, postageConfig, parentJid, tenDigitJobIdIncrementValue);
		CreateUkMailResources ukm = new CreateUkMailResources(customers, postageConfig, productionConfig, cc.getDpsAccuracy(), runNo,actualMailProduct );
		sortCustomers(customers, new CustomerComparatorOriginalOrder());
		writeResultsToFile();
	}

	private static void writeResultsToFile() {
		BufferedReader bu = null;
		String readLine;
		try {
			bu = new BufferedReader(new FileReader(new File(input)));
			readLine = bu.readLine();
		} catch (FileNotFoundException e) {
			LOGGER.fatal("FileNotFoundException thrown when trying to open file '{}' error: '{}'", input, e.getMessage());
			
		} catch (IOException e) {
			LOGGER.fatal("IOException thrown when trying to open file '{}' error: '{}'", input, e.getMessage());
			System.exit(1);
		}
		
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
		int batchTypeIdx = fileMap.get(batchType);
		int totalNumberOfPagesInGroupFieldIdx = fileMap.get(totalNumberOfPagesInGroupField);
		int insertHopperCodeFieldIdx = fileMap.get(insertHopperCodeField);
		int tenDigitJidIdx = fileMap.get(tenDigitJid);
		
		try {
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
						if( customers.get(i).getSot() != null){
							list.add("" + customers.get(i).getSot());
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
					}else if( x ==batchTypeIdx){
						list.add(customers.get(i).getBatchType());
					}else if( x == totalNumberOfPagesInGroupFieldIdx ){
						list.add("" + customers.get(i).getTotalPagesInGroup());
					}else if( x == insertHopperCodeFieldIdx && !(customers.get(i).getInsertRef().trim().isEmpty()) ){
						list.add("" + il.getLookup().get(customers.get(i).getInsertRef().trim()).getHopperCode() );
					}else if( x == tenDigitJidIdx ){
						list.add("" + customers.get(i).getTenDigitJid());
					}else {
						list.add(split[x]);
					}
				}
				fh.write(list);
				i++;
			}
		} catch (IOException e) {
			LOGGER.fatal("IOException thrown when trying to process file '{}' error: '{}'", input, e.getMessage());
			System.exit(1);
		}
		fh.closeFile();
	}

	private static void calculateActualMailProduct(CheckCompliance cc) {

		//Calculate how we are actually going to send this run, UNSORTED, SORTED via MM, SORTED via OCR
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
					//LOGGER.info("Changing batch type '{}' to UNSORTED",customer.getBatchType());
					customer.updateBatchType("UNSORTED", presLookup);
					
				}
				customer.setProduct(actualMailProduct);
			}
		} else if( "OCR".equalsIgnoreCase(productionConfig.getMailsortProduct()) ){
			actualMailProduct="OCR";
			for(Customer customer : customers){
				//SET FINAL ENVELOPE
				if("SORTED".equalsIgnoreCase(customer.getBatchType()) || "MULTI".equalsIgnoreCase(customer.getBatchType()) ){
					if("E".equalsIgnoreCase(customer.getLang()) ){
						customer.setEnvelope(productionConfig.getEnvelopeEnglishOcr());
					}else{
						customer.setEnvelope(productionConfig.getEnvelopeWelshOcr());
					}
					customer.setProduct(actualMailProduct);
				}else if("UNSORTED".equalsIgnoreCase(customer.getBatchType())){
					if("E".equalsIgnoreCase(customer.getLang()) ){
						customer.setEnvelope(productionConfig.getEnvelopeEnglishUnsorted());
					}else{
						customer.setEnvelope(productionConfig.getEnvelopeWelshUnsorted());
					}
					customer.setProduct("UNSORTED");
				}else if("CLERICAL".equalsIgnoreCase(customer.getBatchType()) || "FLEET".equalsIgnoreCase(customer.getBatchType()) ){
					customer.setEnvelope("");
					customer.setProduct("");
				}
			}
		} else if( "MM".equalsIgnoreCase(productionConfig.getMailsortProduct()) ){
			if( cc.getDpsAccuracy() < postageConfig.getUkmMinimumCompliance() ){
				actualMailProduct="OCR";
				for(Customer customer : customers){
					//SET FINAL ENVELOPE
					if("SORTED".equalsIgnoreCase(customer.getBatchType()) || "MULTI".equalsIgnoreCase(customer.getBatchType()) ){
						if("E".equalsIgnoreCase(customer.getLang()) ){
							customer.setEnvelope(productionConfig.getEnvelopeEnglishOcr());
						}else{
							customer.setEnvelope(productionConfig.getEnvelopeWelshOcr());
						}
						customer.setProduct(actualMailProduct);
					}else if("UNSORTED".equalsIgnoreCase(customer.getBatchType())){
						if("E".equalsIgnoreCase(customer.getLang()) ){
							customer.setEnvelope(productionConfig.getEnvelopeEnglishUnsorted());
						}else{
							customer.setEnvelope(productionConfig.getEnvelopeWelshUnsorted());
						}
						customer.setProduct("UNSORTED");
					}else if("CLERICAL".equalsIgnoreCase(customer.getBatchType()) || "FLEET".equalsIgnoreCase(customer.getBatchType()) ){
						customer.setEnvelope("");
						customer.setProduct("");
					}
				}
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
					if("SORTED".equalsIgnoreCase(customer.getBatchType()) || "MULTI".equalsIgnoreCase(customer.getBatchType()) ){
						if("E".equalsIgnoreCase(customer.getLang()) ){
							customer.setEnvelope(productionConfig.getEnvelopeEnglishMm());
						}else{
							customer.setEnvelope(productionConfig.getEnvelopeWelshMm());
						}
						customer.setProduct(actualMailProduct);
					}else if("UNSORTED".equalsIgnoreCase(customer.getBatchType())){
						if("E".equalsIgnoreCase(customer.getLang()) ){
							customer.setEnvelope(productionConfig.getEnvelopeEnglishUnsorted());
						}else{
							customer.setEnvelope(productionConfig.getEnvelopeWelshUnsorted());
						}
						customer.setProduct("UNSORTED");
					}else if("CLERICAL".equalsIgnoreCase(customer.getBatchType()) || "FLEET".equalsIgnoreCase(customer.getBatchType()) ){
						customer.setEnvelope("");
						customer.setProduct("");
					}
				}
			}
		} else {
			LOGGER.fatal("Failed to determine mailing product from {}. Mailsort product set to '{}'",productionConfig.getFilename(), productionConfig.getMailsortProduct());
			System.exit(1);
		}
		LOGGER.info("Run will be sent via {} product.",actualMailProduct);
	}

	private static void sortCustomers(ArrayList<Customer> list, Comparator comparator) {
		try{
			Collections.sort(list, comparator);
		}catch (Exception e){
			LOGGER.fatal("Error when sorting: '{}'",e.getMessage());
			System.exit(1);
		}
	}

	private static void generateCustomersFromInputFile() {
		try{
			fh.write(headerRecords);

			File f = new File(input);
	        BufferedReader b = new BufferedReader(new FileReader(f));
	        String readLine = b.readLine();
	        //Read headers
	        LOGGER.debug("Read line as header '{}'",readLine);

	        fileMap = fh.getMapping();
	        customers = new ArrayList<Customer>();
	        boolean firstCustomer = true;
			presLookup = new HashMap<String,Integer>();
			String presConfig ="";
			String batchComparator = "";
			int custCounter=0;
			
	        while ((readLine = b.readLine()) != null) {
	        	String[] split = readLine.split("\\t",-1);
	        	if(firstCustomer){
					//Create Map of presentation priorities
					if (lookup.get(split[fileMap.get(selectorRef)]) == null){
						LOGGER.fatal("Selector '{}' not found in lookup '{}'",split[fileMap.get(selectorRef)],lookupFile);
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
						split[fileMap.get(mscField)]);
				
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
				customer.setMmCustomerContent(split[fileMap.get(mmCustomerContent)]);
				
				
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
		} catch (IOException e) {
			LOGGER.fatal(e.getMessage());
			System.exit(1);
		}
		
		
	}

	private static void ensureRequiredPropsAreSet(List<String> headers) {
		
		//reqFields is used to validate input, the Y signifies that the field should be present in the input file
		List<DocPropField> reqFields = new ArrayList<DocPropField>();
		reqFields.add(new DocPropField(docRef, "documentReference", true));
		reqFields.add(new DocPropField(noOfPages, "noOfPagesField", true));
		reqFields.add(new DocPropField(lang, "languageFieldName", true));
		reqFields.add(new DocPropField(stat, "stationeryFieldName", true));
		reqFields.add(new DocPropField(batchType, "batchTypeFieldName", true));
		reqFields.add(new DocPropField(subBatch, "subBatchTypeFieldName", true));
		reqFields.add(new DocPropField(selectorRef, "lookupReferenceFieldName", true));
		reqFields.add(new DocPropField(site, "siteFieldName", true));
		reqFields.add(new DocPropField(fleetNo, "fleetNoFieldName", true));
		reqFields.add(new DocPropField(groupId, "groupIdFieldName", true));
		reqFields.add(new DocPropField(paperSize, "paperSizeFieldName", true));
		reqFields.add(new DocPropField(jidField, "jobIdFieldName", true));
		reqFields.add(new DocPropField(mscField, "mscFieldName", true));
		reqFields.add(new DocPropField(presentationPriorityConfigPath, "presentationPriorityConfigPath", false));
		reqFields.add(new DocPropField(presentationPriorityFileSuffix, "presentationPriorityFileSuffix", false));
		reqFields.add(new DocPropField(productionConfigPath, "productionConfigPath", false));
		reqFields.add(new DocPropField(productionFileSuffix, "productionFileSuffix", false));
		reqFields.add(new DocPropField(postageConfigPath, "postageConfigPath", false));
		reqFields.add(new DocPropField(postageFileSuffix, "postageFileSuffix", false));
		reqFields.add(new DocPropField(sortField, "sortField", true));
		reqFields.add(new DocPropField(name1Field, "name1Field", true));
		reqFields.add(new DocPropField(name2Field, "name2Field", true));
		reqFields.add(new DocPropField(add1Field, "address1Field", true));
		reqFields.add(new DocPropField(add2Field, "address2Field", true));
		reqFields.add(new DocPropField(add3Field, "address3Field", true));
		reqFields.add(new DocPropField(add4Field, "address4Field", true));
		reqFields.add(new DocPropField(add5Field, "address5Field", true));
		reqFields.add(new DocPropField(pcField, "postCodeField", true));
		reqFields.add(new DocPropField(dpsField, "dpsField", true));
		reqFields.add(new DocPropField(insertLookup, "insertLookup", false));
		reqFields.add(new DocPropField(envelopeLookup, "envelopeLookup", false));
		reqFields.add(new DocPropField(stationeryLookup, "stationeryLookup", false));
		reqFields.add(new DocPropField(insertField, "insertField", true));
		reqFields.add(new DocPropField(mmBarContent, "mailMarkBarcodeContent", true));
		reqFields.add(new DocPropField(eogField, "eogField", true));
		reqFields.add(new DocPropField(eotField, "eotField", true));
		reqFields.add(new DocPropField(seqField, "childSequence", true));
		reqFields.add(new DocPropField(outEnv, "outerEnvelope", true));
		reqFields.add(new DocPropField(mailingProduct, "mailingProduct", true));
		reqFields.add(new DocPropField(totalNumberOfPagesInGroupField, "totalNumberOfPagesInGroupField", true));
		reqFields.add(new DocPropField(insertHopperCodeField, "insertHopperCodeField", true));
		reqFields.add(new DocPropField(mmCustomerContent, "mailMarkBarcodeCustomerContent", true));
		reqFields.add(new DocPropField(tenDigitJid, "TenDigitJobId", true));
		reqFields.add(new DocPropField(tenDigitJobIdIncrementValue, "tenDigitJobIdIncrementValue",false));
		
		
		
		for(DocPropField requiredField : reqFields){

			if ( requiredField.getAttibuteValue() == null || "null".equals(requiredField.getAttibuteValue())){
				LOGGER.fatal("Field '{}' not in properties file {}.",requiredField.getAttibuteName(), propsFile);
				System.exit(1);
			}else{
				if( !(headers.contains(requiredField.getAttibuteValue())) && requiredField.isRequiredInInputFile() ){
					LOGGER.fatal("Field '{}' not found in input file {}.",requiredField.getAttibuteValue(),input);
					System.exit(1);
				}
			}
		}
	}

	private static void assignPropsFromPropsFile() {
		docRef = CONFIG.getProperty("documentReference");
		noOfPages =CONFIG.getProperty("noOfPagesField");
		lang = CONFIG.getProperty("languageFieldName");
		stat = CONFIG.getProperty("stationeryFieldName");
		batchType = CONFIG.getProperty("batchTypeFieldName");
		subBatch = CONFIG.getProperty("subBatchTypeFieldName");
		selectorRef = CONFIG.getProperty("lookupReferenceFieldName");
		site = CONFIG.getProperty("siteFieldName");
		fleetNo = CONFIG.getProperty("fleetNoFieldName");
		groupId = CONFIG.getProperty("groupIdFieldName");
		paperSize = CONFIG.getProperty("paperSizeFieldName");
		jidField = CONFIG.getProperty("jobIdFieldName");
		mscField = CONFIG.getProperty("mscFieldName");
		presentationPriorityConfigPath = CONFIG.getProperty("presentationPriorityConfigPath");
		presentationPriorityFileSuffix = CONFIG.getProperty("presentationPriorityFileSuffix");
		productionConfigPath = CONFIG.getProperty("productionConfigPath");
		productionFileSuffix = CONFIG.getProperty("productionFileSuffix");
		postageConfigPath = CONFIG.getProperty("postageConfigPath");
		postageFileSuffix = CONFIG.getProperty("postageFileSuffix");
		sortField = CONFIG.getProperty("sortField");
		name1Field = CONFIG.getProperty("name1Field");
		name2Field = CONFIG.getProperty("name2Field");
		add1Field = CONFIG.getProperty("address1Field");
		add2Field = CONFIG.getProperty("address2Field");
		add3Field = CONFIG.getProperty("address3Field");
		add4Field = CONFIG.getProperty("address4Field");
		add5Field = CONFIG.getProperty("address5Field");
		pcField = CONFIG.getProperty("postCodeField");
		dpsField = CONFIG.getProperty("dpsField");
		insertLookup = CONFIG.getProperty("insertLookup");
		envelopeLookup = CONFIG.getProperty("envelopeLookup");
		stationeryLookup = CONFIG.getProperty("stationeryLookup");
		insertField = CONFIG.getProperty("insertField");
		mmBarContent = CONFIG.getProperty("mailMarkBarcodeContent");
		eogField = CONFIG.getProperty("eogField");
		eotField = CONFIG.getProperty("eotField");
		seqField = CONFIG.getProperty("childSequence");
		outEnv = CONFIG.getProperty("outerEnvelope");
		mailingProduct = CONFIG.getProperty("mailingProduct");
		totalNumberOfPagesInGroupField = CONFIG.getProperty("totalNumberOfPagesInGroupField");
		insertHopperCodeField = CONFIG.getProperty("insertHopperCodeField");
		mmCustomerContent = CONFIG.getProperty("mailMarkBarcodeCustomerContent");
		tenDigitJid = CONFIG.getProperty("tenDigitJobId");
		tenDigitJobIdIncrementValue = CONFIG.getProperty("tenDigitJobIdIncrementValue");
	}

	private static void loadSelectorLookupFile() {
		lookupFile=CONFIG.getProperty("lookupFile");
		if( new File(lookupFile).exists()){
			LOGGER.debug("lookupfile='{}' Config='{}",lookupFile,CONFIG.size());
			lookup = new SelectorLookup(lookupFile, CONFIG);
		}else{
			LOGGER.fatal("Lookup file='{}' doesn't exist",lookupFile);
			System.exit(1);
		}
	}

	private static void loadPropertiesFile() {
		CONFIG = new Properties();
		try {
			CONFIG.load(new FileInputStream(propsFile));
		} catch (IOException e) {
			LOGGER.fatal("Log file '{}' didn't load: '{}'",propsFile, e.getMessage());
			System.exit(1);
		}
	}

	private static void validateInputs() {
		if( !(new File(input).exists()) ){
			LOGGER.fatal("File '{}' doesn't exist",input);
			System.exit(1);
		}
		if( !(new File(propsFile).exists()) ){
			LOGGER.fatal("File '{}' doesn't exist",propsFile);
			System.exit(1);
		}
	}

	private static void assignArgs(String[] args) {
		input = args[0];
		output = args[1];
		propsFile = args[2];
		jid = args[3];
		runNo = args[4];
		parentJid = args[5];
	}

	private static void validateNumberOfArgs(String[] args) {
		if( args.length != EXPECTED_NO_OF_ARGS ){
			LOGGER.fatal("Incorrect number of args parsed '{}' expecting '{}'. Args are 1.input file, 2.output file, 3.props file, 4.jobId, 5.Runno 6.ParentJid.",args.length,EXPECTED_NO_OF_ARGS);
			System.exit(1);
		}
	}
}
