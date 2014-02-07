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
	private Integer item_id;
	private Integer mfhd_id;
	private Integer bib_id;
	
}
