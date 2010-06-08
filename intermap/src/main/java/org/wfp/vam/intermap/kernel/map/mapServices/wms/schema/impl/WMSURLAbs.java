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

package org.wfp.vam.intermap.kernel.map.mapServices.wms.schema.impl;

import org.jdom.Element;
import org.wfp.vam.intermap.kernel.map.mapServices.wms.schema.type.WMSFormat;
import org.wfp.vam.intermap.kernel.map.mapServices.wms.schema.type.WMSOnlineResource;
import org.wfp.vam.intermap.kernel.map.mapServices.wms.schema.type.WMSURLif;

/**
 * @author ETj
 */
public abstract class WMSURLAbs implements WMSURLif
{
	protected WMSFormat _format = null;
	protected WMSOnlineResource _onlineResource = null;

	protected static void parse(WMSURLAbs url, Element eURL)
	{
		url.setFormat(eURL.getChildText("Format"));
		url.setOnlineResource(WMSFactory.parseOnlineResource(eURL.getChild("OnlineResource")));
	}

	/**
	 * Sets Format
	 */
	public void setFormat(String format)
	{
		WMSFormat oformat = WMSFormat.parse(format);
		setFormat(oformat);
	}

	public void setFormat(WMSFormat format)
	{
		_format = format;
	}

	/**
	 * Returns Format
	 */
	public WMSFormat getFormat()
	{
		return _format;
	}

	/**
	 * Sets OnlineResource
	 */
	public void setOnlineResource(WMSOnlineResource onlineResource)
	{
		_onlineResource = onlineResource;
	}

	/**
	 * Returns OnlineResource
	 */
	public WMSOnlineResource getOnlineResource()
	{
		return _onlineResource;
	}

}
