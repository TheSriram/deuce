
import sqlite3
import uuid
import collections

MOCK_CASSANDRA_SCHEMA = ["""
CREATE TABLE vaults (
  projectid TEXT,
  vaultid TEXT,
  PRIMARY KEY(projectid, vaultid)
);
""", """
CREATE TABLE blocks (
  projectid TEXT,
  vaultid TEXT,
  blockid TEXT,
  storageid TEXT,
  blocksize INT,
  reftime DATETIME,
  isinvalid BOOLEAN,
  PRIMARY KEY(projectid, vaultid, blockid)
);
""", """
CREATE TABLE files (
  projectid TEXT,
  vaultid TEXT,
  fileid TEXT,
  size INT,
  finalized BOOLEAN,
  PRIMARY KEY(projectid, vaultid, fileid)
);
""", """
CREATE INDEX finalized_index
    on files (finalized);
""", """
CREATE TABLE fileblocks (
  projectid TEXT,
  vaultid TEXT,
  fileid TEXT,
  blockid TEXT,
  blocksize INT,
  offset INT,
  PRIMARY KEY(projectid, vaultid, fileid, offset)
);
""", """
CREATE TABLE blockfiles (
  projectid TEXT,
  vaultid TEXT,
  fileid TEXT,
  blockid TEXT,
  PRIMARY KEY(projectid, vaultid, fileid, blockid)
);
""", """
CREATE TABLE blockreferences (
  projectid TEXT,
  vaultid TEXT,
  blockid TEXT,
  refcount INTEGER,
  PRIMARY KEY(projectid, vaultid, blockid)
);
"""]

from deuce.tests.mock_cassandra import Session


class Cluster(object):

    def __init__(self, contact_points, auth_provider, ssl_options):
        # Create the mock driver in memory only
        self._sqliteconn = sqlite3.connect(':memory:')
        self.cluster_contact_points = contact_points

        for stmt in MOCK_CASSANDRA_SCHEMA:
            self._sqliteconn.execute(stmt)

    @property
    def contact_points(self):
        return self.cluster_contact_points

    def connect(self, keyspace):
        return Session(self._sqliteconn)
