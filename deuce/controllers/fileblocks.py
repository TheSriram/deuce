import itertools
from collections import defaultdict

from pecan import conf, expose, request, response
from pecan.rest import RestController
from pecan.core import abort

import deuce
from deuce.util import set_qs
from deuce.model import Vault, File, Block

from deuce.controllers.validation import *

BLOCK_ID_LENGTH = 40


class FileBlocksController(RestController):

    """The FileBlocksController is responsible for:
    Listing blocks belong to a particular file
    """
    @expose('json')
    @validate(vault_id=VaultGetRule, file_id=FileGetRule,
        marker=OffsetMarkerRule, limit=LimitRule)
    def get_all(self, vault_id, file_id):

        response.headers["Transaction-ID"] = request.context.request_id
        vault = Vault.get(request.project_id, vault_id)

        assert vault is not None

        f = vault.get_file(file_id)

        if not f:
            abort(404, headers={"Transaction-ID": request.context.request_id})

        inmarker = int(request.params.get('marker', 0))
        limit = int(request.params.get('limit',
           conf.api_configuration.max_returned_num))

        # Get the block generator from the metadata driver.
        # Note: +1 on limit is to fetch one past the limt
        # for the purpose of determining if the
        # list was truncated
        retblks = deuce.metadata_driver.create_file_block_generator(
            request.project_id, vault_id, file_id, inmarker, limit + 1)

        resp = defaultdict(list)

        # NOTE(TheSriram): Every record consists of blockid and offset
        # record[0] -> block id
        # record[1] -> offset

        for record in retblks:
            resp[record[0]].append(record[1])
        truncated = len(resp) > 0 and len(resp) == limit + 1

        outmarker = None
        if truncated:
            # Note(ThSriram): This would find the maximum offset in
            # the current batch
            outmarker = max((max(offset)
                             for offset in itertools.chain(resp.values())))

        if outmarker:
            query_args = {'marker': outmarker}
            query_args['limit'] = limit

            returl = set_qs(request.url, query_args)
            response.headers["X-Next-Batch"] = returl

        return resp
