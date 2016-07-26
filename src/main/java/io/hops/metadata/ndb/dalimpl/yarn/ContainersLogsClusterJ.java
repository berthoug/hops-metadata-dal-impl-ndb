/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2015  hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package io.hops.metadata.ndb.dalimpl.yarn;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.ContainersLogsDataAccess;
import io.hops.metadata.yarn.entity.ContainersLogs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ContainersLogsClusterJ implements
        TablesDef.ContainersLogsTableDef,
        ContainersLogsDataAccess<ContainersLogs> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface ContainersLogsDTO {

    @PrimaryKey
    @Column(name = CONTAINERID)
    String getcontainerid();

    void setcontainerid(String containerid);

    @Column(name = START)
    long getstart();

    void setstart(long start);

    @Column(name = STOP)
    long getstop();

    void setstop(long stop);

    @Column(name = EXITSTATUS)
    int getexitstatus();

    void setexitstatus(int exitstate);
    
    @Column(name = PRICE)
    float getPrice();

    void setPrice(float price);

    

    
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public void addAll(Collection<ContainersLogs> containersLogs) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainersLogsDTO> toAdd = new ArrayList<ContainersLogsDTO>();

    for (ContainersLogs entry : containersLogs) {
      toAdd.add(createPersistable(entry, session));
    }

    session.savePersistentAll(toAdd);
    session.release(toAdd);
  }

  @Override
  public void removeAll(Collection<ContainersLogs> containersLogs) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<ContainersLogsDTO> toRemove = new ArrayList<ContainersLogsDTO>();

    for (ContainersLogs entry : containersLogs) {
      toRemove.add(createPersistable(entry, session));
    }

    session.deletePersistentAll(toRemove);
    session.flush();
    session.release(toRemove);
  }
  
  @Override
  public Map<String, ContainersLogs> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<ContainersLogsDTO> dobj = qb.createQueryDefinition(
            ContainersLogsDTO.class);
    HopsQuery<ContainersLogsDTO> query = session.createQuery(dobj);

    List<ContainersLogsDTO> queryResults = query.getResultList();
    Map<String, ContainersLogs> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  @Override
  public Map<String, ContainersLogs> getByExitStatus(int exitstatus) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<ContainersLogsDTO> dobj = qb.createQueryDefinition(
            ContainersLogsDTO.class);
    HopsQuery<ContainersLogsDTO> query = session.createQuery(dobj);
    // Search by unfinished container statuses, to avoid retrieving all
    query.setParameter(EXITSTATUS, exitstatus);

    List<ContainersLogsDTO> queryResults = query.getResultList();
    Map<String, ContainersLogs> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }

  private ContainersLogsDTO createPersistable(ContainersLogs hopCL,
          HopsSession session) throws StorageException {
    ContainersLogsDTO clDTO = session.newInstance(ContainersLogsDTO.class);

    //Set values to persist new ContainersLogs
    clDTO.setcontainerid(hopCL.getContainerid());
    clDTO.setstart(hopCL.getStart());
    clDTO.setstop(hopCL.getStop());
    clDTO.setexitstatus(hopCL.getExitstatus());
    clDTO.setPrice(hopCL.getPrice());
    return clDTO;
  }

  private static Map<String, ContainersLogs> createMap(
          List<ContainersLogsDTO> results) {
    Map<String, ContainersLogs> map = new HashMap<String, ContainersLogs>();
    for (ContainersLogsDTO persistable : results) {
      ContainersLogs hop = createHopContainersLogs(persistable);
      map.put(hop.getContainerid(), hop);
    }
    return map;
  }

  private static ContainersLogs createHopContainersLogs(
          ContainersLogsDTO clDTO) {
    ContainersLogs hop = new ContainersLogs(
            clDTO.getcontainerid(),
            clDTO.getstart(),
            clDTO.getstop(),
            clDTO.getexitstatus(), 
            clDTO.getPrice()
    );
    return hop;
  }
}
