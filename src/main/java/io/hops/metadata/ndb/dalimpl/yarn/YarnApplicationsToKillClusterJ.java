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
import io.hops.metadata.yarn.TablesDef.YarnApplicationsToKillTableDef;
import static io.hops.metadata.yarn.TablesDef.YarnApplicationsToKillTableDef.TABLE_NAME;
import static io.hops.metadata.yarn.TablesDef.YarnApplicationsToKillTableDef.APPLICATIONID;
import static io.hops.metadata.yarn.TablesDef.YarnApplicationsToKillTableDef.PENDING_EVENT_ID;
import io.hops.metadata.yarn.dal.YarnApplicationsToKillDataAccess;
import io.hops.metadata.yarn.entity.ContainersLogs;
import io.hops.metadata.yarn.entity.YarnApplicationsToKill;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author rizvi
 */
public class YarnApplicationsToKillClusterJ implements
        YarnApplicationsToKillTableDef,
        YarnApplicationsToKillDataAccess<YarnApplicationsToKill> {
    
    private static final Log LOG = LogFactory.getLog(YarnApplicationsToKillClusterJ.class);

    @PersistenceCapable(table = TABLE_NAME)
    public interface YarnApplicationsToKillDTO {

        @PrimaryKey
        @Column(name = APPLICATIONID)
        String getApplicationId();
        void setApplicationId(String applicationid);  
        
        @Column(name = PENDING_EVENT_ID)
        int getPendingeventid();
        void setPendingeventid(int pendingeventid);
        
        @Column(name = RMNODEID)
        String getRmnodeId();
        void setRmnodeId(String rmnodeId);  
        
    
    }

    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public Map<String, YarnApplicationsToKill> getAll() throws StorageException {
        LOG.debug("HOP :: ClusterJ YarnApplicationsToKill.getAll - START");
        HopsSession session = connector.obtainSession();
        HopsQueryBuilder qb = session.getQueryBuilder();
//all of your code need to be formated
        HopsQueryDomainType<YarnApplicationsToKillClusterJ.YarnApplicationsToKillDTO> dobj= qb.createQueryDefinition(YarnApplicationsToKillClusterJ.YarnApplicationsToKillDTO.class);
        HopsQuery<YarnApplicationsToKillClusterJ.YarnApplicationsToKillDTO> query = session.createQuery(dobj);

        List<YarnApplicationsToKillClusterJ.YarnApplicationsToKillDTO> queryResults = query.getResultList();
        LOG.debug("HOP :: ClusterJ YarnApplicationsToKill.getAll - STOP");
        Map<String, YarnApplicationsToKill> result = createMap(queryResults);
        session.release(queryResults);
        return result;
    }

    public static Map<String, YarnApplicationsToKill> createMap(List<YarnApplicationsToKillClusterJ.YarnApplicationsToKillDTO> results) {
        Map<String, YarnApplicationsToKill> map = new HashMap<String, YarnApplicationsToKill>();
        for (YarnApplicationsToKillClusterJ.YarnApplicationsToKillDTO persistable : results) {
          YarnApplicationsToKill hop = createHopYarnApplicationsToKill(persistable);
          map.put(hop.getApplicationId(), hop);
        }
        return map;
    }

    private static YarnApplicationsToKill createHopYarnApplicationsToKill(YarnApplicationsToKillClusterJ.YarnApplicationsToKillDTO csDTO) {
        YarnApplicationsToKill hop = new YarnApplicationsToKill(csDTO.getPendingeventid(), csDTO.getRmnodeId() ,csDTO.getApplicationId());
        return hop;
    }
    
    @Override
    public void addAll(Collection<YarnApplicationsToKill> YarnApplicationsListToKill) throws StorageException {
        HopsSession session = connector.obtainSession();
        List<YarnApplicationsToKillClusterJ.YarnApplicationsToKillDTO> toAdd = new ArrayList<YarnApplicationsToKillClusterJ.YarnApplicationsToKillDTO>();
        for (YarnApplicationsToKill _yarnProjectsQuota : YarnApplicationsListToKill) {
          toAdd.add(createPersistable(_yarnProjectsQuota, session));
        }
        session.savePersistentAll(toAdd);
        //    session.flush();
        session.release(toAdd);

    }

    private YarnApplicationsToKillClusterJ.YarnApplicationsToKillDTO createPersistable(YarnApplicationsToKill hopPQ,HopsSession session) throws StorageException {
        YarnApplicationsToKillClusterJ.YarnApplicationsToKillDTO pqDTO = session.newInstance(YarnApplicationsToKillClusterJ.YarnApplicationsToKillDTO.class);
        //Set values to persist new ContainerStatus
        pqDTO.setPendingeventid(hopPQ.getPendingEventId());
        pqDTO.setApplicationId(hopPQ.getApplicationId());
        pqDTO.setRmnodeId(hopPQ.getRmnodeId());
        
        return pqDTO;

    }
    
  @Override
  public void removeAll(
          Collection<YarnApplicationsToKill> KilledApplicationsList) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<YarnApplicationsToKillClusterJ.YarnApplicationsToKillDTO> toRemove
            = new ArrayList<YarnApplicationsToKillClusterJ.YarnApplicationsToKillDTO>();

    for (YarnApplicationsToKill entry : KilledApplicationsList) {
      toRemove.add(createPersistable(entry, session));
    }

    session.deletePersistentAll(toRemove);
    session.flush();
    session.release(toRemove);
  }

    
}
