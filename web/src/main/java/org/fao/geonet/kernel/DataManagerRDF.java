 
// TODO: GNU Licence

package org.fao.geonet.kernel;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.StmtIterator; 
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
		
		Model newMetadataModel = createModelFromRDFXML(md);
		
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
		
		System.out.println("Got metadata for " + uuid + " as RDF : ");
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
			TreeMap<String, String> params = new TreeMap<String, String>();
			params.put("fileID", uuid);
			
			mdRDF = Xml.transform(md, appPath + Geonet.Path.STYLESHEETS + FS + "xml2rdf.xsl", params);
		}
		catch(Exception e) {
			System.out.println("Geonetwork.DataManagerRDF - ERROR : Convertion of XMl -> RDF/XML failed");
			Log.error("Geonetwork.DataManagerRDF","ERROR : Convertion of XMl -> RDF/XML failed");
			
			return false;
		}
		
		return updateMetadataFromRDF(mdRDF, uuid);
	}
	
	public boolean updateMetadataFromRDF(Element newRDFmd, String uuid) {
		System.out.println("Updating " + uuid);
		
		String metadataname = "<http://example.org/" + uuid + "/metadata>";
		
		dataset.begin(ReadWrite.WRITE);
		
		QueryExecution qExec = QueryExecutionFactory.create("DESCRIBE " + metadataname, dataset);
		Model oldMetadataModel = qExec.execDescribe();
		Model newMetadataModel = createModelFromRDFXML(newRDFmd);
		
		System.out.println("Old MD as RDF/XML : ");		// DEBUG
		oldMetadataModel.write(System.out);
		System.out.println("\nEnd of DB Dump");
		
		// Get statements that are in the old model but not the new one
		Model diffModel = oldMetadataModel.difference(newMetadataModel); 
		
		StmtIterator stmtIter = diffModel.listStatements();
		
		System.out.println("diff MD as RDF/XML : ");		// DEBUG
		diffModel.write(System.out);
		System.out.println("\nEnd of DB Dump");
		
		// Delete statements from database
		Model currentDB = dataset.getDefaultModel();
		
		while(stmtIter.hasNext()) {
			currentDB.remove(stmtIter.nextStatement());
		}
		
		// Add all statements from new model
		currentDB.add(newMetadataModel);
		
		dataset.commit();
		
		return true;
	}
	
	private Model createModelFromRDFXML(Element md) {
		Model mdModel = ModelFactory.createDefaultModel();
		XMLOutputter xmlOP = new XMLOutputter();
		ByteArrayOutputStream baos = new ByteArrayOutputStream(); 
		
		try {
			System.out.println("Creating Model from RDF/XML Data : ");
			xmlOP.output(md, System.out);		// DEBUG
			System.out.println("\nEnd of RDF/XML");
			xmlOP.output(md, baos);
		}
		catch(Exception e) {
			// TODO: Handle this appropriately
		}
		
		mdModel.read(new ByteArrayInputStream(baos.toByteArray()), null);
		
		return mdModel;
	}
}