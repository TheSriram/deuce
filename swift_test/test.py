from deuce.drivers.swift import swiftstoragedriver
from deuce.tests.util import mockfile
from aiohttp import streams
import asyncio
def main():
    swift_conn = swiftstoragedriver.SwiftStorageDriver('<mosso_storage_url>','<auth_token>','<Tenant_ID>')
    response = swift_conn.create_vault('<Tenant_ID>','<vault_name>')
    print (response)
    response = swift_conn.vault_exists('<Tenant_ID>','<vault_name>')
    print (response)
    # create multiple blocks
    block1 = mockfile.MockFile(100)
    block2 = mockfile.MockFile(100)
    block3 = mockfile.MockFile(100)
    response = swift_conn.store_block('<Tenant_ID>','<vault_name>',[block1.sha1(),block2.sha1(),block3.sha1()],[block1,block2,block3])
    print (response)




if __name__ == '__main__':
    main()
