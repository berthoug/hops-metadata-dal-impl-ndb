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
import io.hops.metadata.yarn.TablesDef.YarnProjectsQuotaTableDef;
import static io.hops.metadata.yarn.TablesDef.YarnProjectsQuotaTableDef.TABLE_NAME;
import static io.hops.metadata.yarn.TablesDef.YarnProjectsQuotaTableDef.PROJECTID;
import static io.hops.metadata.yarn.TablesDef.YarnProjectsQuotaTableDef.CREDIT;
import io.hops.metadata.yarn.dal.YarnProjectsQuotaDataAccess;
import io.hops.metadata.yarn.entity.YarnProjectsQuota;
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
public class YarnProjectsQuotaClusterJ implements
    YarnProjectsQuotaTableDef,
    YarnProjectsQuotaDataAccess<YarnProjectsQuota>{
    
    private static final Log LOG = LogFactory.getLog(YarnProjectsQuotaClusterJ.class);
    
    @PersistenceCapable(table = TABLE_NAME)
    public interface YarnProjectsQuotaDTO {

        @PrimaryKey
        @Column(name = PROJECTID)
        String getProjectid();
        void setProjectid(String projectid);

        @Column(name = CREDIT)
        int getCredit();        
        void setCredit(int credit);      

    }
    
    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public Map<String, YarnProjectsQuota> getAll() throws StorageException {
        LOG.info("HOP :: ClusterJ YarnProjectsQuota.getAll - START");
        HopsSession session = connector.obtainSession();
        HopsQueryBuilder qb = session.getQueryBuilder();
        
        HopsQueryDomainType<YarnProjectsQuotaClusterJ.YarnProjectsQuotaDTO> dobj = qb.createQueryDefinition(YarnProjectsQuotaClusterJ.YarnProjectsQuotaDTO.class);
        HopsQuery<YarnProjectsQuotaClusterJ.YarnProjectsQuotaDTO> query = session.createQuery(dobj);

        List<YarnProjectsQuotaClusterJ.YarnProjectsQuotaDTO> queryResults = query.getResultList();
        LOG.info("HOP :: ClusterJ YarnProjectsQuota.getAll - STOP");
        Map<String, YarnProjectsQuota> result = createMap(queryResults);
        session.release(queryResults);
        return result;
    }
    
    public static Map<String, YarnProjectsQuota> createMap(List<YarnProjectsQuotaClusterJ.YarnProjectsQuotaDTO> results) {
        Map<String, YarnProjectsQuota> map = new HashMap<String, YarnProjectsQuota>();
        for (YarnProjectsQuotaClusterJ.YarnProjectsQuotaDTO persistable : results) {
          YarnProjectsQuota hop = createHopYarnProjectsQuota(persistable);
          map.put(hop.getProjectid(), hop);
        }
        return map;
    }
    
    private static YarnProjectsQuota createHopYarnProjectsQuota(YarnProjectsQuotaClusterJ.YarnProjectsQuotaDTO csDTO) {
        YarnProjectsQuota hop = new YarnProjectsQuota(csDTO.getProjectid(), csDTO.getCredit());
        return hop;
    } 

    @Override
    public void addAll(Collection<YarnProjectsQuota> yarnProjectsQuota) throws StorageException {
        HopsSession session = connector.obtainSession();
        List<YarnProjectsQuotaDTO> toAdd = new ArrayList<YarnProjectsQuotaDTO>();
        for (YarnProjectsQuota _yarnProjectsQuota : yarnProjectsQuota) {
          toAdd.add(createPersistable(_yarnProjectsQuota, session));
        }
        session.savePersistentAll(toAdd);
    //    session.flush();
        session.release(toAdd);
        
    }
    
    private YarnProjectsQuotaDTO createPersistable(YarnProjectsQuota hopPQ, HopsSession session) throws StorageException {
        YarnProjectsQuotaDTO pqDTO = session.newInstance(YarnProjectsQuotaDTO.class);
        //Set values to persist new ContainerStatus
        pqDTO.setProjectid(hopPQ.getProjectid());
        pqDTO.setCredit(hopPQ.getCredit());
        return pqDTO;
        
    }
    
    /*
    @Override
    public void addAll(Collection<ContainerStatus> containersStatus) throws StorageException {
        HopsSession session = connector.obtainSession();
        List<YarnProjectsQuotaDTO> toAdd = new ArrayList<YarnProjectsQuotaDTO>();
        for (YarnProjectsQuota _yarnProjectsQuota : yarnProjectsQuota) {
          toAdd.add(createPersistable(containerStatus, session));
        }
        session.savePersistentAll(toAdd);
    //    session.flush();
        session.release(toAdd);
  }

  private ContainerStatusDTO createPersistable(ContainerStatus hopCS,
      HopsSession session) throws StorageException {
    ContainerStatusDTO csDTO = session.newInstance(ContainerStatusDTO.class);
    //Set values to persist new ContainerStatus
    csDTO.setcontainerid(hopCS.getContainerid());
    csDTO.setstate(hopCS.getState());
    csDTO.setdiagnostics(hopCS.getDiagnostics());
    csDTO.setexitstatus(hopCS.getExitstatus());
    csDTO.setrmnodeid(hopCS.getRMNodeId());
    csDTO.setpendingeventid(hopCS.getPendingEventId());
    return csDTO;
  }
  */
    
}
