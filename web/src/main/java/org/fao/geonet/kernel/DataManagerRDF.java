 
// TODO: GNU Licence

package org.fao.geonet.kernel;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.DatasetImpl;
import com.hp.hpl.jena.tdb.TDBFactory;

import org.fao.geonet.constants.Geonet;
import org.jdom.Element;
import org.jdom.output.XMLOutputter;
import org.jdom.JDOMException;

import jeeves.utils.Log;
import jeeves.utils.Xml;

import java.io.File;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.TreeMap;

public class DataManagerRDF {

	private String appPath;
	private String dbDir;
	private static String FS = File.separator;
	
	private Dataset dataset;
	
	public DataManagerRDF(String dbDir, String appPath) {
		this.appPath = appPath;
		this.dbDir = dbDir;
		
		// Initialise RDF store (Should probably be done elsewhere)
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
			dataset.begin(ReadWrite.READ);
			Model databaseDump = dataset.getDefaultModel();
			System.out.println("Database dump as RDF/XML : ");		// DEBUG
			databaseDump.write(System.out);
			System.out.println("\nEnd of DB Dump");
			dataset.commit();
			
			dataset.close();
			dataset = null;
		}
	}
	
	public String createMetadataFromXML(Element md, String uuid) {
		// Transform XML->RDF
		Element mdRDF = null;
		
		try {
			TreeMap<String, String> params = new TreeMap<String, String>();
			params.put("fileID", uuid);
			
			mdRDF = Xml.transform(md, appPath + Geonet.Path.STYLESHEETS + FS + "xml2rdf.xsl", params);
		}
		catch(Exception e) {
			System.out.println("Geonetwork.DataManagerRDF - ERROR : Convertion of XMl -> RDF/XML failed");
			Log.error("Geonetwork.DataManagerRDF","ERROR : Convertion of XMl -> RDF/XML failed");
			
			return null;
		}
		
		return createMetadataFromRDF(mdRDF, uuid);
	}
	
	public String createMetadataFromRDF(Element md, String uuid) {
		// Convert the RDF/XML to an RDF model
		
		Model newMetadataModel = ModelFactory.createDefaultModel();
		XMLOutputter xmlOP = new XMLOutputter();
		ByteArrayOutputStream baos = new ByteArrayOutputStream(); 
		
		try {
			System.out.println("Creating RDF/XML Data : ");
			xmlOP.output(md, System.out);		// DEBUG
			System.out.println("\nEnd of RDF/XML");
			xmlOP.output(md, baos);
		}
		catch(Exception e) {
			// TODO: Handle this appropriately
		}
		
		newMetadataModel.read(new ByteArrayInputStream(baos.toByteArray()), null);
		
		dataset.begin(ReadWrite.WRITE);
		
		// TODO: Update to named model for the metadata
		Model existingMetadata = dataset.getDefaultModel();
		existingMetadata.add(newMetadataModel);
		
		dataset.commit();
		
		return uuid;
	}
	
	public Element getMetadataAsXML(String uuid) {
		Element mdRDF = getMetadataAsRDFXML(uuid);
		Element mdXML = null;
		
		// TODO: Uncomment when rdf2xml.xsl has been written
/*		try {
			mdXML = Xml.transform(mdRDF, appPath + Geonet.Path.STYLESHEETS + FS + "rdf2xml.xsl");
		}
		catch(Exception e) {
			Log.error("Geonetwork.DataManagerRDF","ERROR : Convertion of RDF/XMl -> XML failed");
			
			mdXML = null;
		}*/
		
		return mdXML;
	}
	
	/**
	 * Get metadata out of the RDF database in an RDF/XML format
	 */
	public Element getMetadataAsRDFXML(String uuid) {

		// TODO: Fix this, it is a bit of a cludge 
		String metadataname = "<http://example.org/" + uuid + "/metadata>";
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		Element rdfXml = null;
		
		dataset.begin(ReadWrite.READ);
		
		// Create query
		QueryExecution qExec = QueryExecutionFactory.create("DESCRIBE " + metadataname, dataset);
		
		Model metadataModel = qExec.execDescribe();
		
		dataset.commit();
		
		System.out.println("Got metadata as RDF : ");
		metadataModel.write(System.out);		// DEBUG
		System.out.println("End RDF");
		
		metadataModel.write(baos);
		
		try {
			rdfXml = Xml.loadStream(new ByteArrayInputStream(baos.toByteArray()));
		}
		catch (IOException e) {					// TODO: Deal with errors properly
			rdfXml = null;
		}
		catch (JDOMException e) {
			rdfXml = null;
		}
		
		return rdfXml;
	}
	
	public boolean updateMetadataFromXML(Element md, String uuid) {
		// Transform XML->RDF
		Element mdRDF = null;
		
		try {
			mdRDF = Xml.transform(md, appPath + Geonet.Path.STYLESHEETS + FS + "xml2rdf.xsl");
		}
		catch(Exception e) {
			System.out.println("Geonetwork.DataManagerRDF - ERROR : Convertion of XMl -> RDF/XML failed");
			Log.error("Geonetwork.DataManagerRDF","ERROR : Convertion of XMl -> RDF/XML failed");
			
			return false;
		}
		
		return updateMetadateFromRDF(mdRDF, uuid);
	}
	
	public boolean updateMetadataFromRDF(Element md, String uuid) {
		System.out.println("DataManagerRDF::updateMetadataFromRDF(...) - Unfinished");
		
		return false;
	}
}