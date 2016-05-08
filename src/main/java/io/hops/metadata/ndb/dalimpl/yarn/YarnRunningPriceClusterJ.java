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
import io.hops.metadata.yarn.TablesDef.YarnRunningPriceTableDef;
import io.hops.metadata.yarn.dal.YarnRunningPriceDataAccess;
import io.hops.metadata.yarn.entity.YarnRunningPrice;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//public class YarnRunningPriceClusterJ implements
//        YarnRunningPriceTableDef,
//        YarnRunningPriceDataAccess<YarnRunningPrice>
//{
//  
//  private static final Log LOG = LogFactory.getLog(YarnRunningPriceClusterJ.class);
//=======

public class YarnRunningPriceClusterJ implements
        YarnRunningPriceTableDef,
        YarnRunningPriceDataAccess<YarnRunningPrice> {

  private static final Log LOG = LogFactory.getLog(
          YarnRunningPriceClusterJ.class);
//>>>>>>> upstream/develop

  @PersistenceCapable(table = TABLE_NAME)
  public interface YarnRunningPriceDTO {

    @PrimaryKey
    @Column(name = ID)
    String getId();

    void setId(String id);

    @Column(name = TIME)
    long getTime();
    void setTime(long time);

    @Column(name = PRICE)
    float getPrice();
    void setPrice(float price);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

//<<<<<<< HEAD
//
//  @Override
//  public Map<Integer, YarnRunningPrice> getAll() throws StorageException {
//=======
  @Override
  public Map<YarnRunningPrice.PriceType, YarnRunningPrice> getAll() throws
          StorageException {
//>>>>>>> upstream/develop
    LOG.debug("HOP :: ClusterJ YarnRunningPrice.getAll - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<YarnRunningPriceClusterJ.YarnRunningPriceDTO> dobj
            = qb.createQueryDefinition(
                    YarnRunningPriceClusterJ.YarnRunningPriceDTO.class);
    HopsQuery<YarnRunningPriceClusterJ.YarnRunningPriceDTO> query = session.
            createQuery(dobj);

    List<YarnRunningPriceClusterJ.YarnRunningPriceDTO> queryResults = query.
            getResultList();
    LOG.debug("HOP :: ClusterJ YarnRunningPrice.getAll - STOP");
//<<<<<<< HEAD
//    Map<Integer, YarnRunningPrice> result = createMap(queryResults);
//    session.release(queryResults);
//    return result;
//  }
//  
//  public static Map<Integer, YarnRunningPrice> createMap(
//          List<YarnRunningPriceClusterJ.YarnRunningPriceDTO> results) {
//    Map<Integer, YarnRunningPrice> map = new HashMap<Integer, YarnRunningPrice>();
//=======
    Map<YarnRunningPrice.PriceType, YarnRunningPrice> result = createMap(
            queryResults);
    session.release(queryResults);
    return result;
  }

  public static Map<YarnRunningPrice.PriceType, YarnRunningPrice> createMap(
          List<YarnRunningPriceClusterJ.YarnRunningPriceDTO> results) {
    Map<YarnRunningPrice.PriceType, YarnRunningPrice> map
            = new HashMap<YarnRunningPrice.PriceType, YarnRunningPrice>();
//>>>>>>> upstream/develop
    for (YarnRunningPriceClusterJ.YarnRunningPriceDTO persistable : results) {
      YarnRunningPrice hop = createHopYarnRunningPrice(persistable);
      map.put(hop.getId(), hop);
    }
    return map;
  }
//<<<<<<< HEAD
//  
//   private static YarnRunningPrice createHopYarnRunningPrice(
//          YarnRunningPriceClusterJ.YarnRunningPriceDTO csDTO) {
//    YarnRunningPrice hop = new YarnRunningPrice(csDTO.getId(), csDTO.
//=======

  private static YarnRunningPrice createHopYarnRunningPrice(
          YarnRunningPriceClusterJ.YarnRunningPriceDTO csDTO) {
    YarnRunningPrice hop = new YarnRunningPrice(YarnRunningPrice.PriceType.
            valueOf(csDTO.getId()), csDTO.
//>>>>>>> upstream/develop
            getTime(), csDTO.getPrice());
    return hop;
  }

  @Override
  public void add(YarnRunningPrice yarnRunningPrice) throws StorageException {
    HopsSession session = connector.obtainSession();
//<<<<<<< HEAD
//    //List<YarnRunningPriceClusterJ.YarnRunningPriceDTO> toAdd = new ArrayList<YarnRunningPriceClusterJ.YarnRunningPriceDTO>();
//    //for (YarnRunningPrice _yarnProjectsQuota : yarnProjectsQuota) {
//    //  toAdd.add(createPersistable(_yarnProjectsQuota, session));
//    //}
//    YarnRunningPriceClusterJ.YarnRunningPriceDTO toAdd = createPersistable(yarnRunningPrice, session);
//    session.savePersistent(toAdd);
//    //    session.flush();
//    session.release(toAdd);
//  }
//  
//  private YarnRunningPriceClusterJ.YarnRunningPriceDTO createPersistable(YarnRunningPrice hopPQ,
//          HopsSession session) throws StorageException {
//    YarnRunningPriceClusterJ.YarnRunningPriceDTO pqDTO = session.newInstance(YarnRunningPriceClusterJ.YarnRunningPriceDTO.class);
//    //Set values to persist new YarnRunningPriceDTO
//    pqDTO.setId(hopPQ.getId());
//=======
    YarnRunningPriceClusterJ.YarnRunningPriceDTO toAdd = createPersistable(
            yarnRunningPrice, session);
    session.savePersistent(toAdd);
    session.release(toAdd);
  }

  private YarnRunningPriceClusterJ.YarnRunningPriceDTO createPersistable(
          YarnRunningPrice hopPQ,
          HopsSession session) throws StorageException {
    YarnRunningPriceClusterJ.YarnRunningPriceDTO pqDTO = session.newInstance(
            YarnRunningPriceClusterJ.YarnRunningPriceDTO.class);
    //Set values to persist new YarnRunningPriceDTO
    pqDTO.setId(hopPQ.getId().name());
//>>>>>>> upstream/develop
    pqDTO.setTime(hopPQ.getTime());
    pqDTO.setPrice(hopPQ.getPrice());

    return pqDTO;

  }


}
