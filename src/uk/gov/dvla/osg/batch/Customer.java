package uk.gov.dvla.osg.batch;

public class Customer {

	private String groupId, docRef, selectorRef, lang, site, stationery, batchType, subBatch, jid, fleetNo, paperSize, msc;
	private int sequence, batchSequence;
	private Integer presentationPriority;
	
	public Customer(String docRef, String ref, String lang, String stationery, String batchType, String subBatch, String fleetNo, String groupId, String paperSize, String msc){
		this.docRef=docRef;
		this.selectorRef=ref;
		this.lang=lang;
		this.stationery=stationery;
		this.batchType=batchType;
		this.subBatch=subBatch;
		this.fleetNo=fleetNo;
		this.groupId=groupId;
		this.paperSize=paperSize;
		this.msc = msc;
	};
	
	public Integer getPresentationPriority() {
		return presentationPriority;
	}
	public void setPresentationPriority(Integer presentationPriority) {
		this.presentationPriority = presentationPriority;
	}
	public String getMsc() {
		return msc;
	}
	public void setMsc(String msc) {
		this.msc = msc;
	}
	public int getBatchSequence() {
		return batchSequence;
	}
	public void setBatchSequence(int batchSequence) {
		this.batchSequence = batchSequence;
	}
	public String getGroupId() {
		return groupId;
	}
	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}
	public String getFleetNo() {
		return fleetNo;
	}
	public void setFleetNo(String ref) {
		this.fleetNo = ref;
	}
	public String getSelectorRef() {
		return selectorRef;
	}
	public void setSelectorRef(String ref) {
		this.selectorRef = ref;
	}
	public String getDocRef() {
		return docRef;
	}
	public void setDocRef(String ref) {
		this.docRef = ref;
	}
	public String getLang() {
		return lang;
	}
	public void setLang(String lang) {
		this.lang = lang;
	}
	public String getSite() {
		return site;
	}
	public void setSite(String site) {
		this.site = site;
	}
	public String getStationery() {
		return stationery;
	}
	public void setStationery(String stationery) {
		this.stationery = stationery;
	}
	public String getBatchType() {
		return batchType;
	}
	public void setBatchType(String batchType) {
		this.batchType = batchType;
	}
	public String getSubBatch() {
		return subBatch;
	}
	public void setSubBatch(String subBatch) {
		this.subBatch = subBatch;
	}
	public String getJid() {
		return jid;
	}
	public void setJid(String jid) {
		this.jid = jid;
	}
	public int getSequence() {
		return sequence;
	}
	public void setSequence(int sequence) {
		this.sequence = sequence;
	}
	public String[] print(){
		String[] str = {this.docRef,this.site,this.jid};
		return str;
	}
	
	@Override
	public String toString(){
		return docRef + "," + lang + "," + batchType + "," + subBatch + "," + site + "," + fleetNo + "," + msc + "," + groupId + "," + batchSequence + "," + sequence;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((batchType == null) ? 0 : batchType.hashCode());
		result = prime * result + ((lang == null) ? 0 : lang.hashCode());
		result = prime * result
				+ ((paperSize == null) ? 0 : paperSize.hashCode());
		result = prime * result + ((site == null) ? 0 : site.hashCode());
		result = prime * result
				+ ((stationery == null) ? 0 : stationery.hashCode());
		result = prime * result
				+ ((subBatch == null) ? 0 : subBatch.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Customer other = (Customer) obj;
		if (batchType == null) {
			if (other.batchType != null)
				return false;
		} else if (!batchType.equals(other.batchType))
			return false;
		if (lang == null) {
			if (other.lang != null)
				return false;
		} else if (!lang.equals(other.lang))
			return false;
		if (paperSize == null) {
			if (other.paperSize != null)
				return false;
		} else if (!paperSize.equals(other.paperSize))
			return false;
		if (site == null) {
			if (other.site != null)
				return false;
		} else if (!site.equals(other.site))
			return false;
		if (stationery == null) {
			if (other.stationery != null)
				return false;
		} else if (!stationery.equals(other.stationery))
			return false;
		if (subBatch == null) {
			if (other.subBatch != null)
				return false;
		} else if (!subBatch.equals(other.subBatch))
			return false;
		return true;
	}
}
