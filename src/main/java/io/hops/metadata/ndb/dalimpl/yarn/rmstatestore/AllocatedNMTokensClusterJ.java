/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.hops.metadata.ndb.dalimpl.yarn.rmstatestore;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.metadata.yarn.TablesDef;
import io.hops.metadata.yarn.dal.rmstatestore.AllocatedNMTokensDataAccess;
import io.hops.metadata.yarn.entity.rmstatestore.AllocateResponse;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AllocatedNMTokensClusterJ implements
        TablesDef.AllocatedNMTokensTableDef,
        AllocatedNMTokensDataAccess<AllocateResponse> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface AllocatedNMTokenDTO {

    @PrimaryKey
    @Column(name = APPLICATIONATTEMPTID)
    String getapplicationattemptid();

    void setapplicationattemptid(String applicationattemptid);

    @PrimaryKey
    @Column(name = NMTOKEN)
    byte[] getnmtoken();

    void setnmtoken(byte[] nmtoken);
  }

  private final ClusterjConnector connector = ClusterjConnector.getInstance();

  public void update(Collection<AllocateResponse> entries) throws
          StorageException {
    HopsSession session = connector.obtainSession();
    List<AllocatedNMTokenDTO> toPersist
            = new ArrayList<AllocatedNMTokenDTO>();
    for (AllocateResponse resp : entries) {
      //put new values
      toPersist.addAll(createPersistable(resp, session));
      //remove old values
      HopsQueryBuilder qb = session.getQueryBuilder();
      HopsQueryDomainType<AllocatedNMTokenDTO> dobj = qb.
              createQueryDefinition(AllocatedNMTokenDTO.class);
      HopsPredicate pred1 = dobj.get(APPLICATIONATTEMPTID).equal(dobj.param(
              APPLICATIONATTEMPTID));
      dobj.where(pred1);
      HopsQuery<AllocatedNMTokenDTO> query = session.createQuery(dobj);
      query.setParameter("inodeIdParam", resp.getApplicationattemptid());
      query.deletePersistentAll();

    }
    session.savePersistentAll(toPersist);
    session.release(toPersist);
  }

  public Map<String, List<byte[]>> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<AllocatedNMTokenDTO> dobj = qb.createQueryDefinition(AllocatedNMTokenDTO.class);
    HopsQuery<AllocatedNMTokenDTO> query = session.createQuery(dobj);
    List<AllocatedNMTokenDTO> queryResults = query.getResultList();
    Map<String, List<byte[]>> result = createHopAllocatedNMTokensMap(
            queryResults);
    session.release(queryResults);
    return result;
  }

  private List<AllocatedNMTokenDTO> createPersistable(AllocateResponse hop,
          HopsSession session) throws StorageException {
    List<AllocatedNMTokenDTO> result = new ArrayList<AllocatedNMTokenDTO>();
    for (byte[] nmToken : hop.getAllocatedNMTokens()) {
      AllocatedNMTokenDTO allocatedContainerDTO = session.newInstance(AllocatedNMTokenDTO.class);
      allocatedContainerDTO.setapplicationattemptid(hop.
              getApplicationattemptid());
      allocatedContainerDTO.setnmtoken(nmToken);
      result.add(allocatedContainerDTO);
    }
    return result;
  }

  private Map<String, List<byte[]>> createHopAllocatedNMTokensMap(
          List<AllocatedNMTokenDTO> list) throws StorageException {
    Map<String, List<byte[]>> allocatedContainersMap
            = new HashMap<String, List<byte[]>>();

    for (AllocatedNMTokenDTO dto : list) {
      if (allocatedContainersMap.get(dto.getapplicationattemptid()) == null) {
        allocatedContainersMap.put(dto.getapplicationattemptid(),
                new ArrayList<byte[]>());
      }
      allocatedContainersMap.get(dto.getapplicationattemptid()).add(dto.
              getnmtoken());
    }
    return allocatedContainersMap;
  }
}
