import unittest
import httplib, urllib
import simplejson as json

from testfixture import CandlepinTests
from candlepinapi import *

class ConsumerTests(CandlepinTests):

    def setUp(self):
        CandlepinTests.setUp(self)

    def tearDown(self):
        CandlepinTests.tearDown(self)

    def test_list_cert_serials(self):
        result = self.cp.getCertificateSerials(self.uuid)
        print result
        #self.assertTrue('serial' in result)
        #serials = result['serial']
        for serial in result:
            self.assertTrue('serial' in serial)
        # TODO: Could use some more testing here once entitlement certs
        # are actually being generated.

    def test_list_certs(self):
        self.cp.bindProduct(self.uuid, 'monitoring')
        self.cp.bindProduct(self.uuid, 'virtualization_host')

        cert_list = self.cp.getCertificates(self.uuid)
        self.assertEquals(2, len(cert_list))
        last_key = None
        last_cert = None
        for cert in cert_list:
            print
            print "cert from cert_list"
            print cert
            self.assert_ent_cert_struct(cert)
            print

            if last_key is not None:
                self.assertEqual(last_key, cert['key'])
                self.assertNotEqual(last_cert, cert['cert'])
                self.assertEquals(last_key, self.id_cert['key'])
            last_key = cert['key']
            last_cert = cert['cert']

    def test_list_certs_serial_filtering(self):
        self.cp.bindProduct(self.uuid, 'monitoring')
        self.cp.bindProduct(self.uuid, 'virtualization_host')
        self.cp.bindProduct(self.uuid, 'provisioning')

        cert_list = self.cp.getCertificates(self.uuid)
        self.assertEquals(3, len(cert_list))
        for cert in cert_list:
            print
            print "cert from cert_list"
            print cert
            self.assert_ent_cert_struct(cert)
            print

        serials = [cert_list[0]['serial'], cert_list[1]['serial']]
        cert_list = self.cp.getCertificates(self.uuid, serials)
        self.assertEquals(2, len(cert_list))

    def test_uuid(self):
        self.assertTrue(self.uuid != None)

    # Just need to request a product that will fail, for now creating a virt
    # system and trying to give it virtualization_host:
    def test_failed_bind_by_entitlement_pool(self):
        # Create a virt system:
        virt_uuid = self.create_consumer(consumer_type="virt_system")[0]

        # Get pool ID for virtualization_host:
        virt_host = 'virtualization_host'
        results = self.cp.getPools(consumer=self.uuid, product=virt_host)
        print("Virt host pool: %s" % results)
        self.assertEquals(1, len(results))
        pool_id = results[0]['id']

        # Request a virtualization_host entitlement:
        try:
            self.cp.bindPool(virt_uuid, pool_id)
            self.fail("Shouldn't have made it here.")
        except CandlepinException, e:
            self.assertEquals('rulefailed.virt.ents.only.for.physical.systems',
                    e.response.read())
            self.assertEquals(403, e.response.status)

        # Now list consumer's entitlements:
        result = self.cp.getEntitlements(virt_uuid)
        self.assertEquals(type([]), type(result))
        self.assertEquals(0, len(result))

    def test_list_entitlements_product_filtering(self):
        self.cp.bindProduct(self.uuid, 'virtualization_host')
        result = self.cp.getEntitlements(self.uuid)
        self.assertEquals(1, len(result))

        self.cp.bindProduct(self.uuid, 'monitoring')
        result = self.cp.getEntitlements(self.uuid)
        self.assertEquals(2, len(result))

        result = self.cp.getEntitlements(self.uuid, 
                product_id='monitoring')
        self.assertEquals(1, len(result))

    def test_list_pools(self):
        pools = self.cp.getPools(consumer=self.uuid, product="monitoring")
        self.assertEquals(1, len(pools))

    def test_list_pools_for_bad_objects(self):
        self.assertRaises(Exception, self.cp.getPools, consumer='blah')
        self.assertRaises(Exception, self.cp.getPools, owner='-1')

    def test_list_pools_for_bad_query_combo(self):
        # This isn't allowed, should give bad request.
        self.assertRaises(Exception, self.cp.getPools, consumer=self.uuid, owner=1)
