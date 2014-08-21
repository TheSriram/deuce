
from pecan.hooks import PecanHook
from pecan.core import abort

import deuce


class OpenStackHook(PecanHook):
    """Every request that hits Deuce must have a header specifying the
    auth_token for the user the request is for.

    If a request does not provide the header the request should fail
    with a 401"""

    def on_route(self, state):

        class OpenStackObject(object):
            pass

        deuce.context.openstack = OpenStackObject()
        deuce.context.openstack.swift = OpenStackObject()

        # Enforce the existence of the x-auth-token header and assign
        # the value to the deuce context's open stack context
        try:

            # auth_token is assumed validated by an outside source (e.g Repose
            # or WSGI middleware
            deuce.context.openstack.auth_token = \
                state.request.headers['x-auth-token']
            deuce.context.openstack.swift.storage_url = \
                state.request.headers['x-storage-url']
        except KeyError:
            # Invalid request
            abort(401, comment="Possible Missing Headers : "
                               "X-Auth-Token and X-Storage-URL",
                headers={
                    'Transaction-ID': deuce.context.transaction.request_id
                })
