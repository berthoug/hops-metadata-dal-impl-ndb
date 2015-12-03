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
import static io.hops.metadata.ndb.dalimpl.yarn.ContainerStatusClusterJ.createMap;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.TablesDef.YarnContainersLogsTableDef;
import static io.hops.metadata.yarn.TablesDef.YarnContainersLogsTableDef.TABLE_NAME;
import static io.hops.metadata.yarn.TablesDef.YarnContainersLogsTableDef.CONTAINERID;
import static io.hops.metadata.yarn.TablesDef.YarnContainersLogsTableDef.START;
import static io.hops.metadata.yarn.TablesDef.YarnContainersLogsTableDef.STOP;
import static io.hops.metadata.yarn.TablesDef.YarnContainersLogsTableDef.STATE;
import io.hops.metadata.yarn.dal.YarnContainersLogsDataAccess;
import io.hops.metadata.yarn.entity.ContainerStatus;
import io.hops.metadata.yarn.entity.YarnContainersLogs;
import java.util.ArrayList;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author rizvi
 */
public class YarnContainersLogsClusterJ implements
    YarnContainersLogsTableDef,
    YarnContainersLogsDataAccess<YarnContainersLogs> {
        
    private static final Log LOG = LogFactory.getLog(YarnContainersLogsClusterJ.class);
    
    @PersistenceCapable(table = TABLE_NAME)
    public interface YarnContainersLogsDTO {

        @PrimaryKey
        @Column(name = CONTAINERID)
        String getContainerid();
        void setContainerid(String containerid);

        @Column(name = STATE)
        String getstate();
        void setstate(String state);

        @Column(name = START)
        int getStart();        
        void setStart(int start);

        @Column(name = STOP)
        int getStop();        
        void setStop(int stop);    
        

    }

    private final ClusterjConnector connector = ClusterjConnector.getInstance();
        
    @Override
    public Map<String, YarnContainersLogs> getAll() throws StorageException {
        LOG.info("HOP :: ClusterJ YarnContainersLogs.getAll - START");
        HopsSession session = connector.obtainSession();
        HopsQueryBuilder qb = session.getQueryBuilder();

        HopsQueryDomainType<YarnContainersLogsClusterJ.YarnContainersLogsDTO> dobj = qb.createQueryDefinition(YarnContainersLogsClusterJ.YarnContainersLogsDTO.class);
        HopsQuery<YarnContainersLogsClusterJ.YarnContainersLogsDTO> query = session.createQuery(dobj);

        List<YarnContainersLogsClusterJ.YarnContainersLogsDTO> queryResults = query.getResultList();
        LOG.info("HOP :: ClusterJ YarnContainersLogs.getAll - STOP");
        Map<String, YarnContainersLogs> result = createMap(queryResults);
        session.release(queryResults);
        return result;
    }
    
    public static Map<String, YarnContainersLogs> createMap(List<YarnContainersLogsClusterJ.YarnContainersLogsDTO> results) {
        Map<String, YarnContainersLogs> map = new HashMap<String, YarnContainersLogs>();
        for (YarnContainersLogsClusterJ.YarnContainersLogsDTO persistable : results) {
          YarnContainersLogs hop = createHopYarnContainersLogs(persistable);
          map.put(hop.getContainerid(), hop);
        }
        return map;
    }
    
    private static YarnContainersLogs createHopYarnContainersLogs(YarnContainersLogsClusterJ.YarnContainersLogsDTO csDTO) {
        YarnContainersLogs hop = new YarnContainersLogs(csDTO.getContainerid(), csDTO.getstate(), csDTO.getStart(), csDTO.getStop());
        return hop;
    }

    @Override
    public void addAll(Collection<YarnContainersLogs> YarnContainersLogs) throws StorageException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }   
    
    @Override
    public void removeAll(Collection<YarnContainersLogs> removed)
      throws StorageException {
        HopsSession session = connector.obtainSession();
        List<YarnContainersLogsDTO> toRemove = new ArrayList<YarnContainersLogsDTO>();
        for (YarnContainersLogs hop : removed) {      
          toRemove.add(createPersistable(hop,session));
        }    
        session.deletePersistentAll(toRemove);
        session.release(toRemove);
    }
    
    private YarnContainersLogsDTO createPersistable(YarnContainersLogs hopPQ, HopsSession session) throws StorageException {
        YarnContainersLogsDTO pqDTO = session.newInstance(YarnContainersLogsDTO.class);
        //Set values to persist new ContainerStatus
        pqDTO.setContainerid(hopPQ.getContainerid());
        pqDTO.setStart(hopPQ.getStart());
        pqDTO.setStop(hopPQ.getStop());
        pqDTO.setstate(hopPQ.getState());
        return pqDTO;
        
    }    
}
