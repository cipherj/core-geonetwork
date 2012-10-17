//==============================================================================
//===
//=== DataManagerRDF
//===
//=============================================================================
//===	Copyright (C) 2001-2007 Food and Agriculture Organization of the
//===	United Nations (FAO-UN), United Nations World Food Programme (WFP)
//===	and United Nations Environment Programme (UNEP)
//===
//===	This program is free software; you can redistribute it and/or modify
//===	it under the terms of the GNU General Public License as published by
//===	the Free Software Foundation; either version 2 of the License, or (at
//===	your option) any later version.
//===
//===	This program is distributed in the hope that it will be useful, but
//===	WITHOUT ANY WARRANTY; without even the implied warranty of
//===	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
//===	General Public License for more details.
//===
//===	You should have received a copy of the GNU General Public License
//===	along with this program; if not, write to the Free Software
//===	Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA
//===
//===	Contact: Jeroen Ticheler - FAO - Viale delle Terme di Caracalla 2,
//===	Rome - Italy. email: geonetwork@osgeo.org
//==============================================================================

package org.fao.geonet.kernel;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.core.DatasetImpl;
import com.hp.hpl.jena.tdb.TDB;
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
	private TreeMap<String, String> rdfPrefixes;
	
	/**
	 * Initialises the RDF datastore
	 * 
	 * @param dbDir The directory where the RDF datastore is located
	 * @param appPath The application path
	 */
	public DataManagerRDF(String dbDir, String appPath) {
		this.appPath = appPath;
		this.dbDir = dbDir;
		
		rdfPrefixes = new TreeMap<String, String>();
		rdfPrefixes.put("ci", "http://def.seegrid.csiro.au/isotc211/iso19115/2003/citation#");
		rdfPrefixes.put("dq", "http://def.seegrid.csiro.au/isotc211/iso19115/2003/dataquality#");
		rdfPrefixes.put("ex", "http://def.seegrid.csiro.au/isotc211/iso19115/2003/extent#");
		rdfPrefixes.put("gco", "http://www.isotc211.org/2005/gco");
		rdfPrefixes.put("gmd", "http://www.isotc211.org/2005/gmd");
		rdfPrefixes.put("gml", "http://www.opengis.net/gml");
		rdfPrefixes.put("li", "http://def.seegrid.csiro.au/isotc211/iso19115/2003/lineage#");
		rdfPrefixes.put("md", "http://def.seegrid.csiro.au/isotc211/iso19115/2003/metadata#");
		rdfPrefixes.put("owl", "http://www.w3.org/2002/07/owl#");
		rdfPrefixes.put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
		rdfPrefixes.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
		rdfPrefixes.put("skos", "http://www.w3.org/2004/02/skos/core#");
		rdfPrefixes.put("srv", "http://www.isotc211.org/2005/srv");
		rdfPrefixes.put("tm", "http://def.seegrid.csiro.au/isotc211/iso19115/2003/temporalobject");
		rdfPrefixes.put("xsd", "http://www.w3.org/2001/XMLSchema#");
		rdfPrefixes.put("xsl", "http://www.w3.org/1999/XSL/Transform");
		
		// Initialise RDF store (Should probably be done elsewhere)
		dataset = null;
		openDatabase();
	}
	
	/**
	 * Opens the databse creating an empty database if one doesn't already exist
	 */
	private void openDatabase() {
		if(null == dataset) {
			System.out.println("Opening database at : " + dbDir);
			dataset = TDBFactory.createDataset(dbDir);
			
			dataset.begin(ReadWrite.WRITE);
			
			TDB.getContext().set(TDB.symUnionDefaultGraph, true) ; 
			
			dataset.commit();
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
	
	/**
	 * Creates a new entry in the database for the metadata.
	 * 
	 * @param md The metadata as XML
	 * @param uuid The UUID of the metadata
	 */
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
	
	/**
	 * Creates a new entry in the database for the metadata.
	 * 
	 * @param md The metadata as RDF/XML
	 * @param uuid The UUID of the metadata
	 */
	public String createMetadataFromRDF(Element md, String uuid) {
		String metadataName = "<http://example.org/" + uuid + "/metadata>";
		
		// Convert the RDF/XML to an RDF model
		Model newMetadataModel = createModelFromRDFXML(md);
		
		dataset.begin(ReadWrite.WRITE);
		
		if(!dataset.containsNamedModel(metadataName)) {
			dataset.addNamedModel(metadataName, newMetadataModel);
		}
		else {
			dataset.replaceNamedModel(metadataName, newMetadataModel);
		}
		
		dataset.commit();
		
		return uuid;
	}
	
	/**
	 * Deletes metadata from the database
	 * 
	 * @param uuid The UUID of the metadata to be deleted
	 */
	public boolean deleteMetadata(String uuid) {
		String metadataName = "<http://example.org/" + uuid + "/metadata>";
		
		dataset.begin(ReadWrite.WRITE);
		
		if(dataset.containsNamedModel(metadataName)) {
			dataset.removeNamedModel(metadataName);
		}
				
		dataset.commit();
		
		return true;
	}
	
	/**
	 * Gets the metadata specified by uuid from the database as an XML record
	 * 
	 * @param uuid The UUID of the metadata record to retreive
	 */
	public Element getMetadataAsXML(String uuid, String schema) {
		// TODO: Extract metadata based on the schema
		
		Element mdRDF = getMetadataAsRDFXML(uuid);
		Element mdXML = null;
		
		try {
			mdXML = Xml.transform(mdRDF, appPath + Geonet.Path.STYLESHEETS + FS + "rdf2xml.xsl"); 
			
			System.out.println("Metadata as XML after rdf2xml conversion : ");
			XMLOutputter xmlOP = new XMLOutputter();
			xmlOP.output(mdXML, System.out);		// DEBUG
			System.out.println("\nEnd of XML");
		}
		catch(Exception e) {
			Log.error("Geonetwork.DataManagerRDF","ERROR : Convertion of RDF/XMl -> XML failed");
			
			mdXML = null;
		}
		
		return mdXML;
	}
	
	/**
	 * Gets the metadata specified by uuid from the database as an RDF/XML record
	 * 
	 * @param uuid The UUID of the metadata record to retreive
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
		metadataModel.setNsPrefixes(rdfPrefixes);
		
		dataset.commit();
		
		System.out.println("Got metadata for " + uuid + " as RDF : ");
		metadataModel.write(System.out, "RDF/XML-ABBREV");		// DEBUG
		System.out.println("End RDF");
		
		metadataModel.write(baos, "RDF/XML-ABBREV");
		
		try {
			rdfXml = Xml.loadStream(new ByteArrayInputStream(baos.toByteArray()));
		}
		catch (IOException e) {					// TODO: Deal with errors properly
			System.out.println("ERROR : Exception occurred in DataManagerRDF::getMetadataAsRDFXML(...)");
			rdfXml = null;
		}
		catch (JDOMException e) {
			System.out.println("ERROR : Exception occurred in DataManagerRDF::getMetadataAsRDFXML(...)");
			rdfXml = null;
		}
		
		return rdfXml;
	}
	
	/**
	 * Overwrites the metadata specified by uuid with the metadata in md.
	 *  md should be in the standard GeoNetwork XML format. The metadata
	 *  is converted to RDF before being stored.
	 * 
	 * @param md The metadata record in XML
	 * @param uuid The UUID of the record to update
	 */
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
	
	/**
	 * Overwrites the metadata specified by uuid with the metadata in md.
	 *  md should be in RDF/XML
	 * 
	 * @param md The metadata record in RDF/XML
	 * @param uuid The UUID of the record to update
	 */
	public boolean updateMetadataFromRDF(Element newRDFmd, String uuid) {
		System.out.println("Updating " + uuid);
		
		String metadataName = "<http://example.org/" + uuid + "/metadata>";
		
		dataset.begin(ReadWrite.WRITE);
		
		Model newMDModel = createModelFromRDFXML(newRDFmd);
		
		dataset.replaceNamedModel(metadataName, newMDModel);
		
		dataset.commit();
		
		return true;
	}
	
	/**
	 * Creates a Jena Model from the metadata md
	 * 
	 * @param md The metadata record in RDF/XML
	 */
	private Model createModelFromRDFXML(Element md) {
		Model mdModel = ModelFactory.createDefaultModel();
		XMLOutputter xmlOP = new XMLOutputter();
		ByteArrayOutputStream baos = new ByteArrayOutputStream(); 
		
		try {
			System.out.println("Creating Model from RDF/XML Data : ");
			xmlOP.output(md, System.out);		// DEBUG
			System.out.println("\nEnd of RDF/XML");
			xmlOP.output(md, baos);
			mdModel.read(new ByteArrayInputStream(baos.toByteArray()), null);
		}
		catch(Exception e) {
			System.out.println("ERROR : Exception occurred in DataManagerRDF::createModelFromRDFXML(...)");
			// TODO: Handle this appropriately
		}
		
		return mdModel;
	}
}