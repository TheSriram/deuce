import uuid
from functools import lru_cache
from deuce import conf

import deuce

import sys
import importlib
import atexit
import datetime


import itertools
from deuce.drivers.metadatadriver import MetadataStorageDriver, \
    GapError, OverlapError, ConstraintError


class MongoDbStorageDriver(MetadataStorageDriver):

    def __init__(self):

        self._dbfile = conf.metadata_driver.mongodb.path

        self.mongo_pack = importlib.import_module(
            conf.metadata_driver.mongodb.db_module)

        self.client = getattr(self.mongo_pack, 'MongoClient')(
            conf.metadata_driver.mongodb.url)

        self._db = self.client[self._dbfile]
        self._vaults = self._db.vaults
        self._blocks = self._db.blocks
        self._files = self._db.files
        self._fileblocks = self._db.fileblocks
        # Maintain the document size less than the system maximun.
        self._docnum = int(conf.metadata_driver.mongodb.maxFileBlockSegNum)

    def create_vaults_generator(self, marker=None, limit=None):
        """Creates and returns a generator that will return
        the vault IDs.

        :param marker: The vault_id to start of the list
        :param limit: Number of returned items
        """
        self._vaults.ensure_index([('projectid', 1),
            ('vaultid', 1)])
        args = {'projectid': deuce.context.project_id}
        if marker is not None:
            args["vaultid"] = {"$gte": str(marker)}

        limit = self._determine_limit(limit)

        return list(vault["vaultid"] for vault in
            self._vaults.find(args).sort('vaultid', 1).limit(limit))

    def create_vault(self, vault_id):
        """Creates a representation of a vault."""
        args = {
            'projectid': deuce.context.project_id,
            'vaultid': vault_id,
        }
        self._vaults.insert(args)

    def delete_vault(self, vault_id):
        """Deletes the vault from metadata."""
        self._vaults.ensure_index([('projectid', 1),
            ('vaultid', 1)])
        args = {
            'projectid': deuce.context.project_id,
            'vaultid': vault_id,
        }
        self._vaults.remove(args)

    def get_vault_statistics(self, vault_id):
        """Return the statistics on the vault.

        "param vault_id: The ID of the vault to gather statistics for"""
        res = {}

        args = {
            'projectid': deuce.context.project_id,
            'vaultid': vault_id,
        }

        def __stats_get_vault_file_count():
            self._files.ensure_index([('projectid', 1),
                ('vaultid', 1), ('fileid', 1)])
            result = self._files.find(args)
            if result is None:
                return 0  # pragma: no cover
            else:
                return result.count()

        def __stats_get_vault_block_count():
            self._blocks.ensure_index([('projectid', 1),
                ('vaultid', 1), ('blockid', 1)])
            result = self._blocks.find(args)
            if result is None:
                return 0  # pragma: no cover
            else:
                return result.count()

        # TODO: Add any statistics regarding files
        res['files'] = {}
        res['files']['count'] = __stats_get_vault_file_count()

        # TODO: Add any statistics regarding blocks
        res['blocks'] = {}
        res['blocks']['count'] = __stats_get_vault_block_count()

        # TODO: Add any statistics specific to the MongoDB backend
        res['internal'] = {}
        # res['internal']

        return res

    def vault_health(self, vault_id):
        '''Returns the number of bad blocks and bad files associated
        with a vault'''

        args = dict(
            projectid=deuce.context.project_id,
            vaultid=vault_id,
            isinvalid=True
        )
        self._blocks.ensure_index([('projectid', 1),
                ('vaultid', 1)])

        results = self._blocks.find(args)

        bad_blocks = ([res['blockid'] for res in results])
        no_of_bad_blocks = len(bad_blocks)
        bad_files = set()

        self._fileblocks.ensure_index([('projectid', 1),
                ('vaultid', 1), ('blockid', 1)])

        for block_id in bad_blocks:
            args = dict(
                projectid=deuce.context.project_id,
                vaultid=vault_id,
                blockid=block_id,
            )
            results = self._fileblocks.find(args)
            bad_files.update([res['fileid'] for res in results])

        no_of_bad_files = len(bad_files)

        return (no_of_bad_blocks, no_of_bad_files)

    def create_file(self, vault_id, file_id):
        """Creates a new FILES with no blocks and no files"""
        args = {
            'projectid': deuce.context.project_id,
            'vaultid': vault_id,
            'fileid': file_id,
            'finalized': False,
            'size': 0,
            'seq': 0,
            'blocks': []
        }

        self._files.insert(args)

        return file_id

    def file_length(self, vault_id, file_id):
        """Retrieve length the of the file."""
        self._files.ensure_index([('projectid', 1),
            ('vaultid', 1), ('fileid', 1)])
        args = {
            'projectid': deuce.context.project_id,
            'vaultid': vault_id,
            'fileid': file_id
        }
        res = self._files.find_one(args)

        if res is not None:
            length = res.get('size')
            return length
        else:
            return 0

    def get_block_storage_id(self, vault_id, block_id):
        """Retrieve storage id for a given block id"""
        self._blocks.ensure_index([('projectid', 1),
                                  ('vaultid', 1), ('blockid', 1)])
        args = {
            'projectid': deuce.context.project_id,
            'vaultid': vault_id,
            'blockid': block_id
        }

        res = self._blocks.find_one(args)
        if res:
            return str(res.get('storageid'))
        else:
            return None

    def get_block_metadata_id(self, vault_id, storage_id):
        """Retrieve block id for a given storage id"""
        self._blocks.ensure_index([('projectid', 1),
                                  ('vaultid', 1),
                                  ('storageid', 1)])
        args = {
            'projectid': deuce.context.project_id,
            'vaultid': vault_id,
            'storageid': storage_id
        }

        res = self._blocks.find_one(args)
        if res:
            return str(res.get('blockid'))
        else:
            return None

    def has_file(self, vault_id, file_id):
        self._files.ensure_index([('projectid', 1),
            ('vaultid', 1), ('fileid', 1)])
        args = {
            'projectid': deuce.context.project_id,
            'vaultid': vault_id,
            'fileid': file_id
        }

        res = self._files.find_one(args)

        if res is None:
            return False
        return True

    def is_finalized(self, vault_id, file_id):
        self._files.ensure_index([('projectid', 1),
            ('vaultid', 1), ('fileid', 1)])
        args = {
            'projectid': deuce.context.project_id,
            'vaultid': vault_id,
            'fileid': file_id
        }
        res = self._files.find_one(args)

        if res is not None:
            return res.get('finalized')
        return False

    def delete_file(self, vault_id, file_id):
        self._files.ensure_index([('projectid', 1),
            ('vaultid', 1), ('fileid', 1)])
        args = {
            'projectid': deuce.context.project_id,
            'vaultid': vault_id,
            'fileid': file_id
        }

        results = self._fileblocks.find(args)
        block_args = args.copy()
        del block_args['fileid']

        self._blocks.ensure_index([('projectid', 1),
            ('vaultid', 1), ('blockid', 1)])

        for result in results:
            block_args['blockid'] = result['blockid']
            update_args = {
                '$set': {
                    'reftime': int(datetime.datetime.utcnow().timestamp())
                }
            }
            self._blocks.update(block_args, update_args, upsert=False)

        self._files.remove(args)
        self._fileblocks.remove(args)

    def finalize_file(self, vault_id, file_id, file_size=None):
        """Updates FILES to set a file to finalized. This function
        makes no assumptions about whether or not the file record actually
        exists"""
        self._files.ensure_index([('projectid', 1),
            ('vaultid', 1), ('fileid', 1)])

        find_args = {
            'projectid': deuce.context.project_id,
            'vaultid': vault_id,
            'fileid': file_id
        }

        # There could be multiple document for the same file.
        # Need work on one single document a time.
        resfile = self._files.find(find_args)
        if resfile.count() < 1:
            return

        # Check for gap and overlap.
        fileblocks_list = list(self._fileblocks.
            find(find_args).sort('offset', 1))
        expected_offset = 0

        for item in fileblocks_list:
            offset = item['offset']
            blockid = item['blockid']

            blockdata = self.get_block_data(vault_id, blockid)

            if blockdata is None:
                continue

            if offset == expected_offset:
                expected_offset += int(blockdata['blocksize'])
            elif offset < expected_offset:  # Overlap scenario
                raise OverlapError(deuce.context.project_id, vault_id,
                    file_id, blockid, startpos=offset, endpos=expected_offset)
            else:
                raise GapError(deuce.context.project_id, vault_id, file_id,
                    startpos=expected_offset, endpos=offset)

        # Now we must check the very last block
        if file_size and file_size != expected_offset:
            if expected_offset < file_size:
                raise GapError(deuce.context.project_id, vault_id, file_id,
                    expected_offset, file_size)
            else:
                assert expected_offset > file_size

                raise OverlapError(deuce.context.project_id, vault_id, file_id,
                    file_size, startpos=file_size, endpos=expected_offset)

        filerec_id = list(resfile)[0].get('_id')

        # Save finalized state in Files Collection
        if file_size is None:
            file_size = 0

        self._files.update({'_id': filerec_id}, {
            '$set': {
                'finalized': True,
                'size': file_size
            }
        },
            upsert=False)

    def get_file_data(self, vault_id, file_id):
        """Returns a tuple representing data for this file"""
        self._files.ensure_index([('projectid', 1),
            ('vaultid', 1), ('fileid', 1)])
        args = {
            'projectid': deuce.context.project_id,
            'vaultid': vault_id,
            'fileid': file_id
        }

        res = self._files.find_one(args)

        if res is None:
            raise Exception('No such file: {0}'.format(file_id))

        return [res.get('finalized')]

    def mark_block_as_bad(self, vault_id, block_id):
        args = {
            'projectid': deuce.context.project_id, 'vaultid': vault_id,
            'blockid': str(block_id)
        }

        update_args = {
            '$set': {
                'isinvalid': True
            }
        }

        self._blocks.update(args, update_args, upsert=False)

    @staticmethod
    def _block_exists(result, check_status):
        if check_status and result is not None:
            isinvalid = result.get('isinvalid') or False
            return not isinvalid
        else:
            return result is not None

    def has_block(self, vault_id, block_id, check_status=False):
        # Query BLOCKS for the block
        self._blocks.ensure_index([('projectid', 1),
            ('vaultid', 1), ('blockid', 1)])

        args = {
            'projectid': deuce.context.project_id,
            'vaultid': vault_id,
            'blockid': str(block_id)
        }

        res = self._blocks.find_one(args)

        return MongoDbStorageDriver._block_exists(res, check_status)

    # @lru_cache(maxsize=1024)
    def has_blocks(self, vault_id, block_ids, check_status=False):
        # Query BLOCKS for the block
        results = []

        for block_id in block_ids:
            self._blocks.ensure_index([('projectid', 1),
                ('vaultid', 1), ('blockid', 1)])

            args = {
                'projectid': deuce.context.project_id,
                'vaultid': vault_id,
                'blockid': str(block_id)
            }

            result = self._blocks.find_one(args)

            if MongoDbStorageDriver._block_exists(result,
                                                  check_status) is False:
                results.append(block_id)

        return results

    def get_block_data(self, vault_id, block_id):
        """Returns the blocksize for this block"""
        self._blocks.ensure_index([('projectid', 1),
            ('vaultid', 1), ('blockid', 1)])

        args = {
            'projectid': deuce.context.project_id,
            'vaultid': vault_id,
            'blockid': str(block_id)
        }

        return self._blocks.find_one(args)

    def create_block_generator(self, vault_id, marker=None, limit=None):
        self._blocks.ensure_index([('projectid', 1),
            ('vaultid', 1), ('blockid', 1)])
        args = {
            'projectid': deuce.context.project_id,
            'vaultid': vault_id
        }
        if marker is not None:
            args['blockid'] = {'$gte': str(marker)}

        limit = self._determine_limit(limit)

        return list(block['blockid'] for block in
            self._blocks.find(args).sort('blockid', 1).limit(limit))

    def create_file_generator(self, vault_id,
            marker=None, limit=None, finalized=True):
        self._files.ensure_index([('projectid', 1),
            ('vaultid', 1), ('fileid', 1)])
        limit = self._determine_limit(limit)

        args = dict()
        if marker:
            args = {'projectid': deuce.context.project_id, 'vaultid': vault_id,
                'fileid': {'$gte': marker}, 'finalized': finalized}
        else:
            args = {'projectid': deuce.context.project_id, 'vaultid': vault_id,
                'finalized': finalized}

        return list(retfile['fileid'] for retfile in
            self._files.find(args).sort('fileid', 1).limit(limit))

    def create_file_block_generator(self, vault_id, file_id,
            offset=None, limit=None):

        self._fileblocks.ensure_index([('projectid', 1),
            ('vaultid', 1), ('fileid', 1), ('offset', 1)])

        if limit is None:
            limit = 0
        else:
            limit = self._determine_limit(limit)

        search_offset = int(offset) if offset else 0

        args = {
            'projectid': deuce.context.project_id,
            'vaultid': vault_id,
            'fileid': file_id,
            'offset':
                {
                    '$gte': search_offset
                }
        }

        project_args = {
            '_id': 0,
            'blockid': 1,
            'offset': 1
        }
        # This query searches all embedded documents in FILEBLOCKS
        # from the given start point,
        # for the limit number,
        # and sorted by the block offset.

        if limit > 0:
            resblocks = self._fileblocks.find(args,
                project_args).sort('offset', 1).limit(limit)
        else:
            resblocks = self._fileblocks.find(args,
                project_args).sort('offset', 1)

        return ((res['blockid'], res['offset']) for res in resblocks)

    def assign_block(self, vault_id, file_id, block_id, offset):
        # TODO(jdp): tweak this to support multiple assignments
        # TODO(jdp): check for overlaps in metadata
        self._files.ensure_index([('projectid', 1),
            ('vaultid', 1), ('fileid', 1)])
        args = {
            'projectid': deuce.context.project_id,
            'vaultid': vault_id,
            'fileid': file_id,
            'blockid': block_id,
            'offset': offset
        }

        self._fileblocks.update(args, args, upsert=True)
        # Ordered in pymongo.ASCENDING.
        self._fileblocks.ensure_index([('projectid', 1),
            ('vaultid', 1),
            ('fileid', 1),
            ('blockid', 1)])

        # Update the reftime
        block_args = args.copy()
        del block_args['fileid']
        del block_args['offset']
        update_args = {
            '$set': {
                'reftime': int(datetime.datetime.utcnow().timestamp())
            }
        }

        self._blocks.update(block_args, update_args, upsert=False)

    def assign_blocks(self, vault_id, file_id, block_ids, offsets):
        # TODO(jdp): tweak this to support multiple assignments
        # TODO(jdp): check for overlaps in metadata
        for block_id, offset in zip(block_ids, offsets):
            self._files.ensure_index([('projectid', 1),
                ('vaultid', 1), ('fileid', 1)])
            args = {
                'projectid': deuce.context.project_id,
                'vaultid': vault_id,
                'fileid': file_id,
                'blockid': block_id,
                'offset': offset
            }

            self._fileblocks.update(args, args, upsert=True)
            # Ordered in pymongo.ASCENDING.
            self._fileblocks.ensure_index([('projectid', 1),
                ('vaultid', 1),
                ('fileid', 1),
                ('blockid', 1)])

            # Update the reftime
            block_args = args.copy()
            del block_args['fileid']
            del block_args['offset']
            update_args = {
                '$set': {
                    'reftime': int(datetime.datetime.utcnow().timestamp())
                }
            }

            self._blocks.update(block_args, update_args, upsert=False)

    def register_block(self, vault_id, block_id, storage_id, blocksize):
        if not self.has_block(vault_id, block_id):
            args = {
                'projectid': deuce.context.project_id,
                'vaultid': vault_id,
                'blockid': str(block_id),
                'storageid': storage_id,
                'blocksize': blocksize,
                'isinvalid': False,
                'reftime': int(datetime.datetime.utcnow().timestamp())
            }

            self._blocks.update(args, args, upsert=True)

    def unregister_block(self, vault_id, block_id):

        self._require_no_block_refs(vault_id, block_id)

        self._blocks.ensure_index([('projectid', 1),
            ('vaultid', 1), ('blockid', 1)])

        args = {
            'projectid': deuce.context.project_id,
            'vaultid': vault_id,
            'blockid': str(block_id)
        }
        self._blocks.remove(args)

    def get_block_ref_count(self, vault_id, block_id):

        # Blocks can be in two places:
        # 1) The fileblocks collection for unfinalized files
        # 2) The files collection for finalized files

        args = {
            'projectid': deuce.context.project_id,
            'vaultid': vault_id,
            'blockid': str(block_id)
        }

        fileblocks_cnt = self._fileblocks.find(args).count()

        # TODO: we currently count all documents. Let's
        # optimize this query later
        args = {
            'projectid': deuce.context.project_id,
            'vaultid': vault_id
        }

        res = self._files.find(args)

        files_cnt = 0

        for doc in res:
            docgen = (rec['blockid'] for rec in doc['blocks'])
            files_cnt += sum(1 for _ in docgen)

        return files_cnt + fileblocks_cnt

    def get_block_ref_modified(self, vault_id, block_id):

        try:
            blockdata = self.get_block_data(vault_id, block_id)
            return blockdata['reftime']
        except TypeError:
            return 0

    def get_health(self):
        status = ["mongo is active"] if self.client.alive() \
            else ["mongo is not active"]

        return status
