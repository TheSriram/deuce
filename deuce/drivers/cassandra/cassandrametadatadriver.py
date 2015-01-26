
import importlib
import datetime
import six
import ssl
import uuid

from deuce.drivers.metadatadriver import MetadataStorageDriver
from deuce.drivers.metadatadriver import GapError, OverlapError
from deuce.drivers.metadatadriver import ConstraintError
from deuce import conf

import deuce


CQL_CREATE_VAULT = '''
    INSERT INTO vaults (projectid, vaultid)
    VALUES (%(projectid)s, %(vaultid)s)
'''

CQL_DELETE_VAULT = '''
    DELETE FROM vaults
    WHERE projectid = %(projectid)s
    AND vaultid = %(vaultid)s
'''

CQL_GET_ALL_VAULTS = '''
    SELECT vaultid
    FROM vaults
    WHERE projectid = %(projectid)s
    AND vaultid >= %(vaultid)s
    ORDER BY vaultid
    LIMIT %(limit)s
'''

CQL_CREATE_FILE = '''
    INSERT INTO files (projectid, vaultid, fileid, finalized, size)
    VALUES (%(projectid)s, %(vaultid)s, %(fileid)s, false, %(size)s)
'''

CQL_MARK_BLOCK_AS_BAD = '''
    UPDATE blocks SET
    isinvalid = true
    WHERE
    projectid = %(projectid)s AND
    vaultid = %(vaultid)s AND
    blockid = %(blockid)s
'''

CQL_GET_FILE = '''
    SELECT finalized
    FROM files
    WHERE projectid = %(projectid)s
    AND vaultid = %(vaultid)s
    AND fileid = %(fileid)s
'''

CQL_GET_FILE_SIZE = '''
    SELECT size
    FROM files
    WHERE projectid = %(projectid)s
    AND vaultid = %(vaultid)s
    AND fileid = %(fileid)s
'''

CQL_DELETE_FILE = '''
    DELETE FROM files
    WHERE projectid = %(projectid)s
    AND vaultid = %(vaultid)s
    AND fileid = %(fileid)s
'''

CQL_GET_BAD_BLOCKS = '''
    SELECT blockid
    FROM blocks
    WHERE projectid = %(projectid)s
    AND vaultid = %(vaultid)s
    AND isinvalid = true
'''

CQL_GET_FILE_PER_BLOCK = '''
    SELECT fileid
    FROM blockfiles
    WHERE projectid = %(projectid)s
    AND vaultid = %(vaultid)s
    AND blockid = %(blockid)s
'''

CQL_GET_ALL_FILE_BLOCKS = '''
    SELECT blockid, offset
    FROM fileblocks
    WHERE projectid = %(projectid)s
    AND vaultid = %(vaultid)s
    AND fileid = %(fileid)s
    ORDER BY offset
'''

CQL_GET_FILE_BLOCKS = '''
    SELECT blockid, offset
    FROM fileblocks
    WHERE projectid = %(projectid)s
    AND vaultid = %(vaultid)s
    AND fileid = %(fileid)s
    AND offset >= %(marker)s
    ORDER BY offset
    LIMIT %(limit)s
'''

CQL_GET_ALL_FILE_BLOCKS_W_SIZE = '''
    SELECT blockid, offset, blocksize
    FROM fileblocks
    WHERE projectid = %(projectid)s
    AND vaultid = %(vaultid)s
    AND fileid = %(fileid)s
    ORDER by offset
'''

CQL_GET_ALL_BLOCKS = '''
    SELECT blockid
    FROM blocks
    WHERE projectid = %(projectid)s
    AND vaultid = %(vaultid)s
    AND blockid >= %(marker)s
    ORDER BY blockid
    LIMIT %(limit)s
'''

CQL_GET_STORAGE_ID = '''
    SELECT storageid
    FROM blocks
    WHERE projectid = %(projectid)s
    AND vaultid = %(vaultid)s
    AND blockid =%(blockid)s
'''

CQL_GET_BLOCK_ID = '''
    SELECT blockid
    FROM blocks
    WHERE projectid = %(projectid)s
    AND vaultid = %(vaultid)s
    AND storageid =%(storageid)s
'''

CQL_GET_COUNT_ALL_BLOCKS = '''
    SELECT COUNT(*)
    FROM blocks
    WHERE projectid = %(projectid)s
    AND vaultid = %(vaultid)s
'''

CQL_GET_ALL_FILES_MARKER = '''
    SELECT fileid
    FROM files
    WHERE projectid=%(projectid)s
    AND vaultid = %(vaultid)s
    AND fileid >= %(marker)s
    AND finalized = %(finalized)s
    LIMIT %(limit)s
'''

CQL_GET_ALL_FILES = '''
    SELECT fileid
    FROM files
    WHERE projectid=%(projectid)s
    AND vaultid = %(vaultid)s
    AND finalized = %(finalized)s
    LIMIT %(limit)s
'''

CQL_GET_COUNT_ALL_FILES = '''
    SELECT COUNT(*)
    FROM files
    WHERE projectid=%(projectid)s
    AND vaultid = %(vaultid)s
'''

CQL_FINALIZE_FILE = '''
    UPDATE files
    SET finalized=true,
    size=%(size)s
    WHERE projectid=%(projectid)s
    AND vaultid=%(vaultid)s
    AND fileid=%(fileid)s
'''

CQL_ASSIGN_BLOCK_TO_FILE = '''
    INSERT INTO fileblocks
    (projectid, vaultid, fileid, blockid, blocksize, offset)
    VALUES (%(projectid)s, %(vaultid)s, %(fileid)s, %(blockid)s,
    %(blocksize)s, %(offset)s)
'''
CQL_REGISTER_FILE_TO_BLOCK = '''
    INSERT INTO blockfiles
    (projectid, vaultid, fileid, blockid)
    VALUES (%(projectid)s, %(vaultid)s, %(fileid)s, %(blockid)s)
'''
CQL_UNREGISTER_FILE_TO_BLOCK = '''
    DELETE FROM blockfiles
    WHERE projectid=%(projectid)s
    AND vaultid=%(vaultid)s
    AND blockid=%(blockid)s
'''
CQL_REGISTER_BLOCK = '''
    INSERT INTO blocks
    (projectid, vaultid, blockid, storageid, blocksize, isinvalid, reftime)
    VALUES (%(projectid)s, %(vaultid)s, %(blockid)s, %(storageid)s,
    %(blocksize)s, %(isinvalid)s, %(reftime)s)
'''

CQL_UNREGISTER_BLOCK = '''
    DELETE FROM blocks
    WHERE projectid=%(projectid)s
    AND vaultid=%(vaultid)s
    AND blockid=%(blockid)s
'''

CQL_GET_BLOCK_SIZE = '''
    SELECT blocksize FROM blocks
    WHERE projectid = %(projectid)s
    AND vaultid = %(vaultid)s
    AND blockid = %(blockid)s
'''

CQL_GET_BLOCK_REF_COUNT = '''
    SELECT refcount
    FROM blockreferences
    WHERE projectid = %(projectid)s
    AND vaultid = %(vaultid)s
    AND blockid = %(blockid)s
'''

CQL_UPDATE_REF_TIME = '''
    UPDATE blocks
    SET reftime = %(reftime)s
    WHERE projectid = %(projectid)s
    AND vaultid = %(vaultid)s
    AND blockid = %(blockid)s
'''

CQL_GET_BLOCK_REF_TIME = '''
    SELECT reftime
    FROM blocks
    WHERE projectid = %(projectid)s
    AND vaultid = %(vaultid)s
    AND blockid = %(blockid)s
'''

# Note: negative numbers for decrementing works
# fine here.
CQL_INC_BLOCK_REF_COUNT = '''
    UPDATE blockreferences
    SET refcount = refcount + %(delta)s
    WHERE projectid = %(projectid)s
    AND vaultid = %(vaultid)s
    AND blockid = %(blockid)s
'''

CQL_DEL_BLOCK_REF_COUNT = '''
    DELETE FROM blockreferences
    WHERE projectid = %(projectid)s
    AND vaultid = %(vaultid)s
    AND blockid = %(blockid)s
'''

CQL_GET_BLOCK_STATUS = '''
    SELECT isinvalid
    FROM blocks
    WHERE projectid = %(projectid)s
    AND vaultid = %(vaultid)s
    AND blockid = %(blockid)s
'''

CQL_HEALTH_CHECK = '''
    SELECT cluster_name
    FROM system.local
'''


class CassandraStorageDriver(MetadataStorageDriver):

    def __init__(self):

        ssl_options = None
        auth_provider = None

        # Import the driver module.
        self.cassandra = importlib.import_module(
            conf.metadata_driver.cassandra.db_module)

        # Import the cluster submodule
        cluster_module = importlib.import_module(
            '{0}.cluster'.format(conf.metadata_driver.cassandra.db_module))

        # Import the auth submodule
        auth_module = importlib.import_module(
            '{0}.auth'.format(conf.metadata_driver.cassandra.db_module))

        # Import the query submodule
        query_module = importlib.import_module(
            '{0}.query'.format(conf.metadata_driver.cassandra.db_module))

        self.consistency = getattr(self.cassandra,
                                   'ConsistencyLevel')
        self.simplestatement = getattr(query_module,
                                       'SimpleStatement')

        if conf.metadata_driver.cassandra.ssl_enabled:
            ssl_version = getattr(ssl,
                                  conf.metadata_driver.cassandra.tls_version)
            ssl_options = {
                'ca_certs': conf.metadata_driver.cassandra.ssl_ca_certs,
                'ssl_version': ssl_version
            }

        if conf.metadata_driver.cassandra.auth_enabled:
            auth_provider = auth_module.PlainTextAuthProvider(
                username=conf.metadata_driver.cassandra.username,
                password=conf.metadata_driver.cassandra.password
            )

        self._cluster = cluster_module.Cluster(
            contact_points=conf.metadata_driver.cassandra.cluster,
            auth_provider=auth_provider,
            ssl_options=ssl_options)

        # NOTE(TheSriram): We need the total number of nodes in the
        # cluster to be greater than two, if we are going to apply
        # any level of consistency other than ONE

        if len(self._cluster.contact_points) > 2:
            self.consistency_level = getattr(self.consistency,
                conf.metadata_driver.cassandra.consistency)
        else:
            self.consistency_level = self.consistency.ONE

        deuce_keyspace = conf.metadata_driver.cassandra.keyspace
        self._session = self._cluster.connect(deuce_keyspace)

    def create_vault(self, vault_id):
        """Creates a vault"""
        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id
        )

        query = self.simplestatement(CQL_CREATE_VAULT,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)
        return

    def delete_vault(self, vault_id):
        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id
        )
        query = self.simplestatement(CQL_DELETE_VAULT,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)
        return

    def create_vaults_generator(self, marker=None, limit=None):
        args = dict(
            projectid=deuce.context.project_id,
            vaultid=marker or '',
            limit=self._determine_limit(limit)
        )
        query = self.simplestatement(CQL_GET_ALL_VAULTS,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)
        return [row[0] for row in res]

    def get_vault_statistics(self, vault_id):
        """Return the statistics on the vault.

        "param vault_id: The ID of the vault to gather statistics for"""
        res = {}

        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id
        )

        def __stats_query(cql_statement, default_value):
            query = self.simplestatement(cql_statement,
                consistency_level=self.consistency_level)
            result = self._session.execute(query, args)

            try:
                return result[0][0]

            except IndexError:  # pragma: no cover
                return default_value

        def __stats_get_vault_file_count():
            return __stats_query(CQL_GET_COUNT_ALL_FILES, 0)

        def __stats_get_vault_block_count():
            return __stats_query(CQL_GET_COUNT_ALL_BLOCKS, 0)

        # Add any statistics regarding files
        res['files'] = {}
        res['files']['count'] = __stats_get_vault_file_count()

        # Add any statistics regarding blocks
        res['blocks'] = {}
        res['blocks']['count'] = __stats_get_vault_block_count()

        # Add any statistics specific to the Cassandra backend
        res['internal'] = {}

        return res

    def vault_health(self, vault_id):
        '''Returns the number of bad blocks and bad files associated
        with a vault'''

        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
        )

        bad_blocks = self._session.execute(CQL_GET_BAD_BLOCKS, args)

        no_of_bad_blocks = len(bad_blocks)

        bad_files = set()
        results = []

        for block_id in bad_blocks:
            args = dict(
                projectid=deuce.context.project_id,
                vaultid=vault_id,
                blockid=block_id[0],
            )
            future = self._session.execute_async(CQL_GET_FILE_PER_BLOCK, args)
            results.append(future)

        for future in results:
            try:
                bad_files.add(future.result()[0][0])
            except IndexError:
                pass

        no_of_bad_files = len(bad_files)

        return (no_of_bad_blocks, no_of_bad_files)

    def create_file(self, vault_id, file_id):
        """Creates a new file with no blocks and no files"""

        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            fileid=uuid.UUID(file_id),
            size=0
        )

        query = self.simplestatement(CQL_CREATE_FILE,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)

        return file_id

    def file_length(self, vault_id, file_id):
        """Retrieve the length of the file."""
        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            fileid=uuid.UUID(file_id)
        )

        query = self.simplestatement(CQL_GET_FILE_SIZE,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)

        try:
            return int(res[0][0])
        except IndexError:
            return 0

    def get_block_storage_id(self, vault_id, block_id):
        """Retrieve storage id for a given block id"""
        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            blockid=block_id
        )

        query = self.simplestatement(CQL_GET_STORAGE_ID,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)
        try:
            return str(res[0][0])
        except IndexError:
            return None

    def get_block_metadata_id(self, vault_id, storage_id):
        """Retrieve block id for a given storage id"""
        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            storageid=storage_id
        )

        query = self.simplestatement(CQL_GET_BLOCK_ID,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)
        try:
            return str(res[0][0])
        except IndexError:
            return None

    def has_file(self, vault_id, file_id):
        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            fileid=uuid.UUID(file_id)
        )

        query = self.simplestatement(CQL_GET_FILE,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)

        return len(res) > 0

    def is_finalized(self, vault_id, file_id):

        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            fileid=uuid.UUID(file_id)
        )

        query = self.simplestatement(CQL_GET_FILE,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)

        try:
            row = res[0]
            return row[0] == 1
        except IndexError:
            return False

    def _delete_files_from_blockfiles(self, vault_id, blockids):
        futures = []

        query = self.simplestatement(CQL_UNREGISTER_FILE_TO_BLOCK,
            consistency_level=self.consistency_level)

        for blockid in blockids:
            args = dict(
                projectid=deuce.context.project_id,
                vaultid=vault_id,
                blockid=blockid
            )

            future = self._session.execute_async(query,
                                                 args)
            futures.append(future)

        for future in futures:
            future.result()

    def delete_file(self, vault_id, file_id):

        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            fileid=uuid.UUID(file_id)
        )

        query = self.simplestatement(CQL_DELETE_FILE,
            consistency_level=self.consistency_level)
        self._session.execute(query, args)

        # now list the file blocks, delete the mapping from blocks to
        # files and decrement the block reference count

        query = self.simplestatement(CQL_GET_ALL_FILE_BLOCKS_W_SIZE,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)

        block_ids = [data[0] for data in res]
        self._delete_files_from_blockfiles(vault_id,
                                           block_ids)

        self._inc_block_ref_counts(vault_id, block_ids, -1)

    def finalize_file(self, vault_id, file_id, file_size=None):
        """Updates the files table to set a file to finalized. This function
        makes no assumptions about whether or not the file record actually
        exists"""

        # Check for gaps and overlaps.
        expected_offset = 0

        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            fileid=uuid.UUID(file_id)
        )

        query = self.simplestatement(CQL_GET_ALL_FILE_BLOCKS_W_SIZE,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)

        for blockid, offset, size in res:

            # Use one last chance to check for the block size
            # if it is not in the fileblocks row.
            if size is None:
                size = self._get_block_size(vault_id, blockid)

                # If size is None, the block was never registered so we
                # skip this record. This will likely result in a GapError
                # being thrown on the next pass
                if size is None:
                    continue

            if offset == expected_offset:
                expected_offset += size
            elif offset < expected_offset:  # Block overlaps previous block
                raise OverlapError(deuce.context.project_id, vault_id,
                                   file_id, blockid, startpos=offset,
                                   endpos=expected_offset)
            else:  # There is a gap between this block and the previous one
                raise GapError(deuce.context.project_id, vault_id, file_id,
                               startpos=expected_offset, endpos=offset)

        # Now we must check the very last block and ensure
        # that is completes the file. This is only doable if
        # the final file size was provided
        if file_size and file_size != expected_offset:

            if expected_offset < file_size:  # Gap
                raise GapError(deuce.context.project_id, vault_id, file_id,
                               startpos=expected_offset, endpos=file_size)

            else:
                assert expected_offset > file_size

                # This means that the "last" block overlaps
                # the end of the file.
                raise OverlapError(deuce.context.project_id, vault_id, file_id,
                                   blockid, startpos=file_size,
                                   endpos=expected_offset)

        if self.has_file(vault_id, file_id):
            if file_size is None:
                file_size = 0

            args = dict(
                size=file_size,
                projectid=deuce.context.project_id,
                vaultid=vault_id,
                fileid=uuid.UUID(file_id)
            )

            query = self.simplestatement(CQL_FINALIZE_FILE,
                consistency_level=self.consistency_level)
            res = self._session.execute(query, args)

    def get_block_data(self, vault_id, block_id):

        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            blockid=block_id
        )

        query = self.simplestatement(CQL_GET_BLOCK_SIZE,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)

        try:
            return dict(blocksize=res[0][0])
        except IndexError:
            raise Exception("No such block: {0}".format(block_id))

    def _get_block_size(self, vault_id, block_id):
        """Returns the size of the specified block. If the block
        is not found, None is returned"""

        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            blockid=block_id
        )

        query = self.simplestatement(CQL_GET_BLOCK_SIZE,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)

        try:
            return res[0][0]
        except IndexError:
            return None

    def _get_block_sizes(self, vault_id, block_ids):
        """Returns the size of the specified block. If the block
        is not found, None is returned"""

        def get_result(future):
            try:
                return future[0][0]
            except IndexError:
                return None

        futures = []

        query = self.simplestatement(CQL_GET_BLOCK_SIZE,
            consistency_level=self.consistency_level)

        for block_id in block_ids:
            args = dict(
                projectid=deuce.context.project_id,
                vaultid=vault_id,
                blockid=block_id
            )

            future = self._session.execute_async(query, args)
            futures.append(future)
        return [get_result(future.result()) for future in futures]

    def get_file_data(self, vault_id, file_id):
        """Returns a tuple representing data for this file"""
        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            fileid=uuid.UUID(file_id)
        )

        query = self.simplestatement(CQL_GET_FILE,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)

        try:
            row = res[0]
        except IndexError:
            raise Exception("No such file: {0}".format(file_id))

        return row

    def mark_block_as_bad(self, vault_id, block_id):

        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            blockid=block_id
        )

        query = self.simplestatement(CQL_MARK_BLOCK_AS_BAD,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)

    @staticmethod
    def _block_exists(result, check_status):
        """Helper function to check the result of a cassandra
        block query taking into consideration whether or not
        we should be considering the status of the block"""
        if len(result) == 0:  # No blocks exist
            return False

        # There should be exactly one row and one column
        assert len(result) == 1
        assert len(result[0]) == 1

        if check_status and result[0][0] is True:
            return False

        return True

    def has_block(self, vault_id, block_id, check_status=False):
        retval = False

        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            blockid=block_id
        )

        query = self.simplestatement(CQL_GET_BLOCK_STATUS,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)

        return CassandraStorageDriver._block_exists(res, check_status)

    def has_blocks(self, vault_id, block_ids, check_status=False):

        futures = []
        query = self.simplestatement(CQL_GET_BLOCK_STATUS,
            consistency_level=self.consistency_level)

        for block_id in block_ids:
            args = dict(
                projectid=deuce.context.project_id,
                vaultid=vault_id,
                blockid=block_id
            )

            future = self._session.execute_async(query, args)
            futures.append((future, block_id))

        exists = lambda res: CassandraStorageDriver._block_exists(
            res, check_status)

        return [block_id for future, block_id in futures
                if not exists(future.result())]

    def create_block_generator(self, vault_id, marker=None,
                               limit=None):

        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            marker=marker or '',
            limit=self._determine_limit(limit)
        )

        query = self.simplestatement(CQL_GET_ALL_BLOCKS,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)

        return [row[0] for row in res]

    def create_file_generator(self, vault_id, marker=None, limit=None,
                              finalized=True):

        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            finalized=finalized,
            limit=self._determine_limit(limit)
        )

        if marker is None:
            # query = CQL_GET_ALL_FILES
            query = self.simplestatement(CQL_GET_ALL_FILES,
                consistency_level=self.consistency_level)
        else:
            args.update(dict(
                marker=uuid.UUID(marker)
            ))
            query = self.simplestatement(CQL_GET_ALL_FILES_MARKER,
                consistency_level=self.consistency_level)
            # query = CQL_GET_ALL_FILES_MARKER

        res = self._session.execute(query, args)

        return [str(row[0]) for row in res]

    def create_file_block_generator(self, vault_id, file_id,
                                    offset=None, limit=None):

        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            fileid=uuid.UUID(file_id),
        )

        if limit is None:
            # query = CQL_GET_ALL_FILE_BLOCKS
            query = self.simplestatement(CQL_GET_ALL_FILE_BLOCKS,
                consistency_level=self.consistency_level)
        else:

            args.update(dict(
                marker=offset or 0,
                limit=self._determine_limit(limit)
            ))

            # query = CQL_GET_FILE_BLOCKS
            query = self.simplestatement(CQL_GET_FILE_BLOCKS,
                consistency_level=self.consistency_level)

        query_res = self._session.execute(query, args)

        return [(row[0], row[1]) for row in query_res]

    def assign_blocks(self, vault_id, file_id, block_ids, offsets):

        blocksizes = self._get_block_sizes(vault_id, block_ids)

        # Note: blocksize can be None if the block does not yet exist. This
        # will probably not be allowed in the future, but for now we allow
        # this to be compatible with the other drivers.
        futures = []

        file_to_block_query = self.simplestatement(CQL_REGISTER_FILE_TO_BLOCK,
            consistency_level=self.consistency_level)

        block_to_file_query = self.simplestatement(CQL_ASSIGN_BLOCK_TO_FILE,
            consistency_level=self.consistency_level)

        for block_id, blocksize, offset in zip(block_ids, blocksizes,
                                               offsets):
            args = dict(
                projectid=deuce.context.project_id,
                vaultid=vault_id,
                fileid=uuid.UUID(file_id),
                blockid=block_id,
                blocksize=blocksize,
                offset=offset
            )

            fileblocks_future = self._session.execute_async(
                block_to_file_query,
                args)
            futures.append(fileblocks_future)

            blockfile_args = args.copy()

            del blockfile_args['offset']
            del blockfile_args['blocksize']

            blockfiles_future = self._session.execute_async(
                file_to_block_query,
                blockfile_args)

            futures.append(blockfiles_future)

        for future in futures:
            future.result()

        self._inc_block_ref_counts(vault_id, block_ids)

    def assign_block(self, vault_id, file_id, block_id, offset):

        blocksize = self._get_block_size(vault_id, block_id)

        # Note: blocksize can be None if the block does not yet exist. This
        # will probably not be allowed in the future, but for now we allow
        # this to be compatible with the other drivers.
        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            fileid=uuid.UUID(file_id),
            blockid=block_id,
            blocksize=blocksize,
            offset=offset
        )


        block_to_file_query = self.simplestatement(CQL_ASSIGN_BLOCK_TO_FILE,
            consistency_level=self.consistency_level)
        file_to_block_query = self.simplestatement(CQL_REGISTER_FILE_TO_BLOCK,
            consistency_level=self.consistency_level)


        blockfile_args = args.copy()

        del blockfile_args['offset']
        del blockfile_args['blocksize']

        self._session.execute(block_to_file_query, args)
        self._session.execute(file_to_block_query, blockfile_args)

        self._inc_block_ref_count(vault_id, block_id)

    def register_block(self, vault_id, block_id, storage_id, blocksize):
        if not self.has_block(vault_id, block_id):
            args = dict(
                projectid=deuce.context.project_id,
                vaultid=vault_id,
                blockid=block_id,
                storageid=storage_id,
                reftime=int(datetime.datetime.utcnow().timestamp()),
                isinvalid=False,
                blocksize=int(blocksize)
            )

            query = self.simplestatement(CQL_REGISTER_BLOCK,
                consistency_level=self.consistency_level)
            res = self._session.execute(query, args)

    def unregister_block(self, vault_id, block_id):

        self._require_no_block_refs(vault_id, block_id)

        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            blockid=block_id
        )

        query = self.simplestatement(CQL_UNREGISTER_BLOCK,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)

        self._del_block_ref_count(vault_id, block_id)

    def get_block_ref_count(self, vault_id, block_id):

        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            blockid=block_id
        )

        query = self.simplestatement(CQL_GET_BLOCK_REF_COUNT,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)

        try:
            return res[0][0]
        except IndexError:
            return 0

    def _inc_block_ref_counts(self, vault_id, block_ids, cnt=1):

        futures = []
        inc_ref_count_query = self.simplestatement(CQL_INC_BLOCK_REF_COUNT,
            consistency_level=self.consistency_level)

        for block_id in block_ids:
            args = dict(
                projectid=deuce.context.project_id,
                vaultid=vault_id,
                blockid=block_id,
                delta=cnt
            )

            future = self._session.execute_async(inc_ref_count_query, args)
            futures.append(future)

        for future in futures:
            future.result()
        # The Ref-time value is stored in the blocks table
        # if the block doesn't exist then the ref-time insertion
        # will cause it to exist and then the register_block() will
        # fail to insert the data correctly. Therefore, only
        # insert the ref-time if we already have the block
        #
        # Note: the block registration will automatically insert the
        # ref-time as well.

        missing_block_ids = self.has_blocks(vault_id, block_ids)
        update_block_ids = set(block_ids) - set(missing_block_ids)
        futures = []
        update_reftime_query = self.simplestatement(CQL_UPDATE_REF_TIME,
            consistency_level=self.consistency_level)
        for block_id in update_block_ids:

            reftime_args = dict(
                projectid=deuce.context.project_id,
                vaultid=vault_id,
                blockid=block_id,
                reftime=int(datetime.datetime.utcnow().timestamp())
            )
            future = self._session.execute_async(update_reftime_query,
                                                 reftime_args)
            futures.append(future)

        for future in futures:
            future.result()

    def _inc_block_ref_count(self, vault_id, block_id, cnt=1):

        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            blockid=block_id,
            delta=cnt
        )

        query = self.simplestatement(CQL_INC_BLOCK_REF_COUNT,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)

        # The Ref-time value is stored in the blocks table
        # if the block doesn't exist then the ref-time insertion
        # will cause it to exist and then the register_block() will
        # fail to insert the data correctly. Therefore, only
        # insert the ref-time if we already have the block
        #
        # Note: the block registration will automatically insert the
        # ref-time as well.
        if self.has_block(vault_id, block_id):
            reftime_args = dict(
                projectid=deuce.context.project_id,
                vaultid=vault_id,
                blockid=block_id,
                reftime=int(datetime.datetime.utcnow().timestamp())
            )
            query = self.simplestatement(CQL_UPDATE_REF_TIME,
                consistency_level=self.consistency_level)
            res = self._session.execute(query, reftime_args)

    def _del_block_ref_count(self, vault_id, block_id):

        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            blockid=block_id
        )

        query = self.simplestatement(CQL_DEL_BLOCK_REF_COUNT,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)

    def get_block_ref_modified(self, vault_id, block_id):

        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            blockid=block_id
        )

        query = self.simplestatement(CQL_GET_BLOCK_REF_TIME,
            consistency_level=self.consistency_level)
        res = self._session.execute(query, args)

        try:
            return res[0][0]
        except IndexError:
            return 0

    def get_health(self):
        try:
            args = ()
            query = self.simplestatement(CQL_HEALTH_CHECK,
                consistency_level=self.consistency_level)
            res = self._session.execute(query, args)
            return ["cassandra cluster: [{0}] is active".format(res[0][0])]
        except:  # pragma: no cover
            return ["cassandra is not active."]
