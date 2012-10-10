 
// TODO: GNU Licence

package org.fao.geonet.kernel;

import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.sparql.core.DatasetImpl;
import com.hp.hpl.jena.query.Dataset;

public class DataManagerRDF {

	private String dataDir;
	private Dataset dataset;
	
	public DataManagerRDF(String dataDir) {
		this.dataDir = dataDir;
		
		// TODO: Initialise RDF store (Should be done elsewhere)
		dataset = null;
		openDatabase();
	}
	
	/**
	 * Opens the databse
	 */
	private void openDatabase() {
		if(null == dataset) {
			dataset = TDBFactory.createDataset(dataDir);
		}
	}
	
	/**
	 * Closes the database and syncs to disk
	 */
	public void closeDatabase() {
		if(null != dataset) {
			dataset.close();
			dataset = null;
		}
	}
}