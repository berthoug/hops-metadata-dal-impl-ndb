/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
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

/**
 *
 * @author rizvi
 */
public class YarnRunningPriceClusterJ implements
        YarnRunningPriceTableDef,
        YarnRunningPriceDataAccess<YarnRunningPrice>
{
  
  private static final Log LOG = LogFactory.getLog(YarnRunningPriceClusterJ.class);

  @PersistenceCapable(table = TABLE_NAME)
  public interface YarnRunningPriceDTO {

    @PrimaryKey
    @Column(name = ID)
    int getId();
    void setId(int id);

    @Column(name = TIME)
    long getTime();
    void setTime(long time);

    @Column(name = PRICE)
    float getPrice();
    void setPrice(float price);

  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();


  @Override
  public Map<Integer, YarnRunningPrice> getAll() throws StorageException {
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
    Map<Integer, YarnRunningPrice> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }
  
  public static Map<Integer, YarnRunningPrice> createMap(
          List<YarnRunningPriceClusterJ.YarnRunningPriceDTO> results) {
    Map<Integer, YarnRunningPrice> map = new HashMap<Integer, YarnRunningPrice>();
    for (YarnRunningPriceClusterJ.YarnRunningPriceDTO persistable : results) {
      YarnRunningPrice hop = createHopYarnRunningPrice(persistable);
      map.put(hop.getId(), hop);
    }
    return map;
  }
  
   private static YarnRunningPrice createHopYarnRunningPrice(
          YarnRunningPriceClusterJ.YarnRunningPriceDTO csDTO) {
    YarnRunningPrice hop = new YarnRunningPrice(csDTO.getId(), csDTO.
            getTime(), csDTO.getPrice());
    return hop;
  }

  @Override
  public void add(YarnRunningPrice yarnRunningPrice) throws StorageException {
    HopsSession session = connector.obtainSession();
    //List<YarnRunningPriceClusterJ.YarnRunningPriceDTO> toAdd = new ArrayList<YarnRunningPriceClusterJ.YarnRunningPriceDTO>();
    //for (YarnRunningPrice _yarnProjectsQuota : yarnProjectsQuota) {
    //  toAdd.add(createPersistable(_yarnProjectsQuota, session));
    //}
    YarnRunningPriceClusterJ.YarnRunningPriceDTO toAdd = createPersistable(yarnRunningPrice, session);
    session.savePersistent(toAdd);
    //    session.flush();
    session.release(toAdd);
  }
  
  private YarnRunningPriceClusterJ.YarnRunningPriceDTO createPersistable(YarnRunningPrice hopPQ,
          HopsSession session) throws StorageException {
    YarnRunningPriceClusterJ.YarnRunningPriceDTO pqDTO = session.newInstance(YarnRunningPriceClusterJ.YarnRunningPriceDTO.class);
    //Set values to persist new YarnRunningPriceDTO
    pqDTO.setId(hopPQ.getId());
    pqDTO.setTime(hopPQ.getTime());
    pqDTO.setPrice(hopPQ.getPrice());

    return pqDTO;

  }

  
}
