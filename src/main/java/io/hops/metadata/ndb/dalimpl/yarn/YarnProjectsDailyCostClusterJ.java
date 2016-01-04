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
import static io.hops.metadata.yarn.TablesDef.YarnProjectsDailyCostTableDef.PROJECTNAME;
import static io.hops.metadata.yarn.TablesDef.YarnProjectsDailyCostTableDef.USER;
import static io.hops.metadata.yarn.TablesDef.YarnProjectsDailyCostTableDef.DAY;
import static io.hops.metadata.yarn.TablesDef.YarnProjectsDailyCostTableDef.CREDITS_USED;
import io.hops.metadata.yarn.dal.YarnProjectsDailyCostDataAccess;
import io.hops.metadata.yarn.entity.YarnProjectsDailyCost;
import io.hops.metadata.yarn.entity.YarnProjectsDailyCost;
import io.hops.metadata.yarn.entity.YarnProjectsDailyCost;
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
public class YarnProjectsDailyCostClusterJ implements
    TablesDef.YarnProjectsDailyCostTableDef,
    YarnProjectsDailyCostDataAccess<YarnProjectsDailyCost>{
    
    private static final Log LOG = LogFactory.getLog(YarnProjectsDailyCostClusterJ.class);
   
    @PersistenceCapable(table = TABLE_NAME)
    public interface YarnProjectsDailyCostDTO {

        @PrimaryKey
        @Column(name = PROJECTNAME)
        String getProjectName();
        void setProjectName(String projectName);
        
        @PrimaryKey
        @Column(name = USER)
        String getUser();
        void setUser(String user);

        @PrimaryKey
        @Column(name = DAY)
        long getDay();        
        void setDay(long day);
        
        @Column(name = CREDITS_USED)
        int getCreditUsed();        
        void setCreditUsed(int credit);

    }
    
    private final ClusterjConnector connector = ClusterjConnector.getInstance();

    @Override
    public Map<String, YarnProjectsDailyCost> getAll() throws StorageException {
        LOG.info("HOP :: ClusterJ YarnProjectsDailyCost.getAll - START");
        HopsSession session = connector.obtainSession();
        HopsQueryBuilder qb = session.getQueryBuilder();
        
        HopsQueryDomainType<YarnProjectsDailyCostClusterJ.YarnProjectsDailyCostDTO> dobj = qb.createQueryDefinition(YarnProjectsDailyCostClusterJ.YarnProjectsDailyCostDTO.class);
        HopsQuery<YarnProjectsDailyCostClusterJ.YarnProjectsDailyCostDTO> query = session.createQuery(dobj);

        List<YarnProjectsDailyCostClusterJ.YarnProjectsDailyCostDTO> queryResults = query.getResultList();
        LOG.info("HOP :: ClusterJ YarnProjectsDailyCost.getAll - STOP");
        Map<String, YarnProjectsDailyCost> result = createMap(queryResults);
        session.release(queryResults);
        return result;
    }
    
    public static Map<String, YarnProjectsDailyCost> createMap(List<YarnProjectsDailyCostClusterJ.YarnProjectsDailyCostDTO> results) {
        Map<String, YarnProjectsDailyCost> map = new HashMap<String, YarnProjectsDailyCost>();
        for (YarnProjectsDailyCostClusterJ.YarnProjectsDailyCostDTO persistable : results) {
          YarnProjectsDailyCost hop = createHopYarnProjectsDailyCost(persistable);
          map.put(hop.getProjectName() + "#" + hop.getProjectUser() + "#" + hop.getDay(), hop);
          
        }
        return map;
    }
    
    private static YarnProjectsDailyCost createHopYarnProjectsDailyCost(YarnProjectsDailyCostClusterJ.YarnProjectsDailyCostDTO csDTO) {
        YarnProjectsDailyCost hop = new YarnProjectsDailyCost(csDTO.getProjectName(), csDTO.getUser(),csDTO.getDay(), csDTO.getCreditUsed());
        return hop;
    }

    @Override
    public void addAll(Collection<YarnProjectsDailyCost> yarnProjectsDailyCost) throws StorageException {        
        HopsSession session = connector.obtainSession();
        List<YarnProjectsDailyCostClusterJ.YarnProjectsDailyCostDTO> toAdd = new ArrayList<YarnProjectsDailyCostClusterJ.YarnProjectsDailyCostDTO>();
        for (YarnProjectsDailyCost _yarnProjectsDailyCost : yarnProjectsDailyCost) {
          toAdd.add(createPersistable(_yarnProjectsDailyCost, session));
        }
        session.savePersistentAll(toAdd);
    //    session.flush();
        session.release(toAdd); 
    }
    
    private YarnProjectsDailyCostClusterJ.YarnProjectsDailyCostDTO createPersistable(YarnProjectsDailyCost hopPQ, HopsSession session) throws StorageException {
        YarnProjectsDailyCostClusterJ.YarnProjectsDailyCostDTO pqDTO = session.newInstance(YarnProjectsDailyCostClusterJ.YarnProjectsDailyCostDTO.class);
        //Set values to persist new ContainerStatus
        pqDTO.setProjectName(hopPQ.getProjectName());
        pqDTO.setUser(hopPQ.getProjectUser());
        pqDTO.setDay(hopPQ.getDay());
        pqDTO.setCreditUsed(hopPQ.getCreditsUsed());

        return pqDTO;        
    } 
}

