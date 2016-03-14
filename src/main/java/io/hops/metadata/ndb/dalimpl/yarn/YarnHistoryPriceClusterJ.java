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
import io.hops.metadata.yarn.TablesDef;
import static io.hops.metadata.yarn.TablesDef.YarnHistoryPriceTableDef.PRICE;
import static io.hops.metadata.yarn.TablesDef.YarnHistoryPriceTableDef.TABLE_NAME;
import static io.hops.metadata.yarn.TablesDef.YarnHistoryPriceTableDef.TIME;
import io.hops.metadata.yarn.dal.YarnHistoryPriceDataAccess;
import io.hops.metadata.yarn.entity.YarnHistoryPrice;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author rizvi
 */
public class YarnHistoryPriceClusterJ implements
        TablesDef.YarnHistoryPriceTableDef,
        YarnHistoryPriceDataAccess<YarnHistoryPrice>
{
  
  private static final Log LOG = LogFactory.getLog(YarnHistoryPriceClusterJ.class);
  @PersistenceCapable(table = TABLE_NAME)
  public interface YarnHistoryPriceDTO {

    @PrimaryKey
    @Column(name = TIME)
    long getTime();
    void setTime(long time);

    @Column(name = PRICE)
    float getPrice();
    void setPrice(float price);

  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  
  

  @Override
  public Map<Long, YarnHistoryPrice> getAll() throws StorageException {
    LOG.debug("HOP :: ClusterJ YarnHistoryPrice.getAll - START");
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    HopsQueryDomainType<YarnHistoryPriceClusterJ.YarnHistoryPriceDTO> dobj = qb.createQueryDefinition(YarnHistoryPriceClusterJ.YarnHistoryPriceDTO.class);
    HopsQuery<YarnHistoryPriceClusterJ.YarnHistoryPriceDTO> query = session.createQuery(dobj);

    List<YarnHistoryPriceClusterJ.YarnHistoryPriceDTO> queryResults = query.getResultList();
    LOG.debug("HOP :: ClusterJ YarnHistoryPrice.getAll - STOP");
    Map<Long, YarnHistoryPrice> result = createMap(queryResults);
    session.release(queryResults);
    return result;
  }
  
  public static Map<Long, YarnHistoryPrice> createMap(List<YarnHistoryPriceClusterJ.YarnHistoryPriceDTO> results) {
    Map<Long, YarnHistoryPrice> map = new HashMap<Long, YarnHistoryPrice>();
    for (YarnHistoryPriceClusterJ.YarnHistoryPriceDTO persistable : results) {
      YarnHistoryPrice hop = createHopYarnHistoryPrice(persistable);
      map.put(hop.getTime(), hop);
    }
    return map;
  }
  
   private static YarnHistoryPrice createHopYarnHistoryPrice(YarnHistoryPriceClusterJ.YarnHistoryPriceDTO csDTO) {
    YarnHistoryPrice hop = new YarnHistoryPrice(csDTO.getTime(), csDTO.getPrice());
    return hop;
  }

  @Override
  public void add(YarnHistoryPrice yarnHistoryPrice) throws StorageException {
    HopsSession session = connector.obtainSession();
    //List<YarnHistoryPriceClusterJ.YarnHistoryPriceDTO> toAdd = new ArrayList<YarnHistoryPriceClusterJ.YarnHistoryPriceDTO>();
    //for (YarnHistoryPrice _yarnProjectsQuota : yarnProjectsQuota) {
    //  toAdd.add(createPersistable(_yarnProjectsQuota, session));
    //}
    YarnHistoryPriceClusterJ.YarnHistoryPriceDTO toAdd = createPersistable(yarnHistoryPrice, session);
    session.savePersistent(toAdd);
    //    session.flush();
    session.release(toAdd);
  }
  
  private YarnHistoryPriceClusterJ.YarnHistoryPriceDTO createPersistable(YarnHistoryPrice hopPQ,
          HopsSession session) throws StorageException {
    YarnHistoryPriceClusterJ.YarnHistoryPriceDTO pqDTO = session.newInstance(YarnHistoryPriceClusterJ.YarnHistoryPriceDTO.class);
    //Set values to persist new YarnHistoryPriceDTO    
    pqDTO.setTime(hopPQ.getTime());
    pqDTO.setPrice(hopPQ.getPrice());

    return pqDTO;

  }
  
}
