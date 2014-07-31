
from pecan.hooks import PecanHook
from pecan.core import abort


class AuthTokenHook(PecanHook):
    """Every request that hits Deuce must have a header specifying the
    project id that the request is for. The Project ID is synonymous with
    Account ID, Mosso ID, etc. in the Rackspace world.

    If a request does not provide the header the request should fail
    with a 400"""

    def on_route(self, state):

        # Enforce the existence of the x-project-id header and assign
        # the value to the request project id.
        try:
            state.request.auth_token = state.request.headers['x-auth-token']
            # TODO: validate the project_id
        except KeyError:
            # Invalid request
            abort(401, comment="Missing Header : X-Auth-Token",
                  headers={'Transaction-ID': state.request.context.request_id})
