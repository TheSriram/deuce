import deuce.drivers.cassandra.cassandrametadatadriver \
    as actual_driver

import uuid


class ConsistencyLevel(object):

    def __init__(self):
        pass

    ANY = 0
    ONE = 1
    TWO = 2
    THREE = 3
    QUORUM = 4
    ALL = 5
    LOCAL_QUORUM = 6
    EACH_QUORUM = 7
    SERIAL = 8
    LOCAL_SERIAL = 9
    LOCAL_ONE = 10


class Future(object):

    def __init__(self, result):
        self._result = result

    def result(self):
        return [element for element in self._result]


class Session(object):

    def __init__(self, conn):
        self.conn = conn

    def execute(self, query, queryargs):
        # Health check.
        if 'system.local' in query:
            return 'true'

        original_query = query

        query = query.replace('false', '0')
        query = query.replace('true', '1')

        if isinstance(queryargs, tuple):

            # convert UUID to string
            queryargs = tuple([str(s) if isinstance(s, uuid.UUID)
                           else s for s in queryargs])

            # sqlite prefers ? over %s for positional args
            query = query.replace('%s', '?')

        elif isinstance(queryargs, dict):

            # If the user passed dictionary arguments, assume that they
            # used that cassandra %(fieldname)s and convert to sqlite's
            # :fieldname

            for k, v in queryargs.items():
                cass_style_arg = "%({0})s".format(k)
                sqlite_style_arg = ":{0}".format(k)
                query = query.replace(cass_style_arg, sqlite_style_arg)

                # Convert UUID parameters to strings
                if isinstance(v, uuid.UUID):
                    queryargs[k] = str(v)

        if original_query == actual_driver.CQL_INC_BLOCK_REF_COUNT:

            # Special-case this query, since sqlite doesn't
            # support upserts

            insert_query = """
                INSERT or IGNORE into blockreferences
                (projectid, vaultid, blockid, refcount)
                VALUES
                (:projectid, :vaultid, :blockid, :refcount)
            """

            insert_args = queryargs.copy()
            insert_args.update({'refcount': 0})
            del insert_args["delta"]

            self.conn.execute(insert_query, insert_args)

        elif original_query == actual_driver.CQL_UPDATE_REF_TIME or \
                original_query == actual_driver.CQL_REGISTER_BLOCK:

            # unixTimeStampOf() and now() are not part of SQLite
            query = query.replace('unixTimeStampOf(now())',
                                  "strftime('%s', 'now')")

        elif original_query == actual_driver.CQL_HEALTH_CHECK:

            # neither now() nor system.local are part of sqlite
            # So the following gives us the same query as the original
            query = "SELECT strftime('%s', 'now')"

        elif original_query == actual_driver.CQL_REGISTER_FILE_TO_BLOCK:

            query = """
                INSERT or REPLACE INTO blockfiles
                (projectid, vaultid, fileid, blockid)
                VALUES (:projectid, :vaultid, :fileid, :blockid)
            """

        res = self.conn.execute(query, queryargs)
        res = list(res)

        if original_query == actual_driver.CQL_GET_BLOCK_STATUS:
            # Special-case the return value of this query. Returns
            # 1 or 0 and should be true or false
            if res == [(0,)]:
                res = [(False,)]
            elif res == [(1,)]:
                res = [(True,)]
            elif res == []:
                pass  # Do nothing
            else:
                raise Exception("Unexpected result")

        return res

    def execute_async(self, query, queryargs):

        res = self.execute(query, queryargs)

        return Future(res)
