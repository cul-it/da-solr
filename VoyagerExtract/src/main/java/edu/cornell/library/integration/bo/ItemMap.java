package edu.cornell.library.integration.bo;

public class ItemMap {

	public Integer getItemId() {
		return item_id;
	}
	public Integer getMfhdId() {
		return mfhd_id;
	}
	public Integer getBibId() {
		return bib_id;
	}
	public String getModifyDate() {
		return modify_date;
	}
	public void setItemId(Integer item_id) {
		this.item_id = item_id;
	}
	public void setMfhdId(Integer mfhd_id) {
		this.mfhd_id = mfhd_id;
	}
	public void setBibId(Integer bib_id) {
		this.bib_id = bib_id;
	}
	public void setItemId(String item_id) {
		this.item_id = Integer.valueOf(item_id);
	}
	public void setMfhdId(String mfhd_id) {
		this.mfhd_id = Integer.valueOf(mfhd_id);
	}
	public void setBibId(String bib_id) {
		this.bib_id = Integer.valueOf(bib_id);
	}
	public void setModifyDate(String modify_date) {
		if (modify_date == null)
			this.modify_date = "";
		else
			this.modify_date = modify_date;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(item_id);
		sb.append('|');
		sb.append(mfhd_id);
		sb.append('|');
		sb.append(bib_id);
		sb.append('|');
		sb.append(modify_date);
		return sb.toString();
	}
	
	private Integer item_id;
	private Integer mfhd_id;
	private Integer bib_id;
	private String modify_date;
	
}
