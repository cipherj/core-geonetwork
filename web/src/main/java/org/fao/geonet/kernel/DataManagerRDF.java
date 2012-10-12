 
// TODO: GNU Licence

package org.fao.geonet.kernel;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.DatasetImpl;
import com.hp.hpl.jena.tdb.TDBFactory;

import org.fao.geonet.constants.Geonet;
import org.jdom.Element;
import org.jdom.output.XMLOutputter;

import jeeves.utils.Log;
import jeeves.utils.Xml;

import java.io.File;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;


public class DataManagerRDF {

	private String appPath;
	private String dbDir;
	private static String FS = File.separator;
	
	private Dataset dataset;
	
	public DataManagerRDF(String dbDir, String appPath) {
		this.appPath = appPath;
		this.dbDir = dbDir;
		
		// TODO: Initialise RDF store (Should be done elsewhere)
		dataset = null;
		openDatabase();
	}
	
	/**
	 * Opens the databse
	 */
	private void openDatabase() {
		if(null == dataset) {
			System.out.println("Opening database at : " + dbDir);
			dataset = TDBFactory.createDataset(dbDir);
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
	
	public String createMetadataFromXML(Element md, String id) {
		// Transform XML->RDF
		Element mdRDF = null;
		
		try {
			mdRDF = Xml.transform(md, appPath + Geonet.Path.STYLESHEETS + FS + "xml2rdf.xsl");
		}
		catch(Exception e) {
			System.out.println("Geonetwork.DataManagerRDF - ERROR : Convertion of XMl -> RDF/XML failed");
			Log.error("Geonetwork.DataManagerRDF","ERROR : Convertion of XMl -> RDF/XML failed");
			
			return "";
		}
		
		return createMetadataFromRDF(mdRDF, id);
	}
	
	public String createMetadataFromRDF(Element md, String id) {
		// Convert the RDF/XML to an RDF model
		
		Model newMetadataModel = ModelFactory.createDefaultModel();
		XMLOutputter xmlOP = new XMLOutputter();
		ByteArrayOutputStream baos = new ByteArrayOutputStream(); 
		
		try {
			System.out.println("RDF/XML Data : ");
			xmlOP.output(md, System.out);		// DEBUG
			xmlOP.output(md, baos);
		}
		catch(Exception e) {
			// TODO: Handle this appropriately
		}
		
		newMetadataModel.read(new ByteArrayInputStream(baos.toByteArray()), null);
		
		dataset.begin(ReadWrite.WRITE) ;
		
		// TODO: Update to named model for the metadata
		Model existingMetadata = dataset.getDefaultModel();
		existingMetadata.add(newMetadataModel);
		
		dataset.commit();
		
		return id;
	}
	
	public Element getMetadataAsXML(String uuid) {
		Element mdRDF = getMetadataAsRDFXML(uuid);
		Element mdXML = null;
		
		try {
			mdXML = Xml.transform(mdRDF, appPath + Geonet.Path.STYLESHEETS + FS + "rdf2xml.xsl");
		}
		catch(Exception e) {
			Log.error("Geonetwork.DataManagerRDF","ERROR : Convertion of RDF/XMl -> XML failed");
			
			mdXML = new Element("Fail");;
		}
		
		return mdXML;
	}
	
	public Element getMetadataAsRDFXML(String uuid) {
		// TODO: Get metadata from database
		
		return new Element("FIXME");
	}
	
	public boolean updateMetadataFromXML(Element md, String id) {
		
		return false;
	}
}