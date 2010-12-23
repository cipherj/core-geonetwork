//==============================================================================
//===
//=== ThreadPool
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

package org.fao.geonet.util;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;

import jeeves.utils.Log;

import org.fao.geonet.constants.Geonet;
 
public class ThreadPool {
    int poolSize = 5;
 
    int maxPoolSize = 10;
 
    long keepAliveTime = 2;
 
    ThreadPoolExecutor threadPool = null;
 
    final ArrayBlockingQueue<Runnable> queue = 
												new ArrayBlockingQueue<Runnable>(20);
 
		//--- threadpool will create all possible threads and queue tasks up to the
		//--- size of the queue - any tasks submitted after that will be run by
		//--- the caller thread (ie. the main thread) - this is why we create with
		//--- CallerRunsPolicy for rejected tasks
    public ThreadPool() {
        threadPool = new ThreadPoolExecutor(poolSize, maxPoolSize, 
									keepAliveTime, TimeUnit.SECONDS, queue, 
									new ThreadPoolExecutor.CallerRunsPolicy()); 
    }
 
    public void runTask(Runnable task) {
        Log.debug(Geonet.THREADPOOL, "Task count in pool.."+threadPool.getTaskCount() );
        Log.debug(Geonet.THREADPOOL, "Queue Size before assigning the task.."+queue.size() );
        threadPool.execute(task);
        Log.debug(Geonet.THREADPOOL, "Queue Size after assigning the task.."+queue.size() );
        Log.debug(Geonet.THREADPOOL, "Pool Size after assigning the task.."+threadPool.getActiveCount() );
        Log.debug(Geonet.THREADPOOL, "Task count in pool.."+threadPool.getTaskCount() );
		}

		public void shutDown() {
				threadPool.shutdown();
    }
}