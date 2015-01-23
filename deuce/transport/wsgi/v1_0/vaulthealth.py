import json

from stoplight import validate

from deuce.transport.validation import *
from deuce.model import Vault
import deuce.transport.wsgi.errors as errors
import deuce.util.log as logging

logger = logging.getLogger(__name__)


class CollectionResource(object):
    @validate(vault_id=VaultGetRule)
    def on_get(self, req, resp, vault_id):
        """Returns the statistics on vault controller object"""
        vault = Vault.get(vault_id)

        if vault:
            bad_blocks, bad_files = vault.get_vault_health()
            response = {
                'Vault': vault_id,
                'Bad Blocks': bad_blocks,
                'Bad Files': bad_files
            }
            resp.body = json.dumps(response)
        else:
            logger.error('Vault [{0}] does not exist'.format(vault_id))
            raise errors.HTTPNotFound
