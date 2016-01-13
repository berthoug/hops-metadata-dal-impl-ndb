Hops Metadata DAL NDB Implementation
===

Hops Database abstraction layer for storing the hops metadata in MySQL Cluster

How to build
===

```
mvn clean install
```

deploys the jar file as an artifact to the kompics maven repository.

```
./deploy.sh
```

Development Notes
===
Updates to schema/schema.sql should be copied to:
hops-hadoop-chef/templates/default/hops.sql.erb


# License

Hops-Metadata-dal-impl-ndb is released under an [GPL 2.0 license](LICENSE.txt).
