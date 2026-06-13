-- Keyspace para Cloud Provider Analytics (AstraDB / Cassandra)
-- En Astra: verificar nombre del datacenter en la consola (p. ej. dc-1) y ajustar si aplica.
CREATE KEYSPACE IF NOT EXISTS cloud_analytics
WITH REPLICATION = {
  'class': 'NetworkTopologyStrategy',
  'datacenter1': 3
};
