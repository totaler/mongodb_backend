import unittest

from osv import osv, fields
from mongodb_backend import testing, osv_mongodb
from expects import *
from destral.transaction import Transaction

import doctest
from mongodb_backend import mongodb2
from mongodb_backend import orm_mongodb


def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocTestSuite(mongodb2))
    return tests


def test_compute_order_parsing(self):
    class order_test(orm_mongodb.orm_mongodb):
        _name = 'order.test'

    with Transaction().start(self.database) as txn:
        cursor = txn.cursor
        uid = txn.user

        testing_class = order_test(cursor)

        res = testing_class._compute_order(cursor, uid, 'test desc')
        self.assertEqual(res, [('test', -1)])

        res = testing_class._compute_order(cursor, uid, 'test asc')
        self.assertEqual(res, [('test', 1)])

        res = testing_class._compute_order(cursor, uid, 'test desc, '
                                                        'test2 desc')
        self.assertEqual(res, [('test', -1), ('test2', -1)])

        res = testing_class._compute_order(cursor, uid, 'test asc, '
                                                        'test2 desc')
        self.assertEqual(res, [('test', 1), ('test2', -1)])


class MongoModelTest(osv_mongodb.osv_mongodb):
    _name = 'mongomodel.test'

    _columns = {
        'name': fields.char('Name', size=64),
        'other_name': fields.char('Other name', size=64),
        'boolean_field': fields.boolean('Boolean Field', size=64)
    }


class MongoDBBackendTest(testing.MongoDBTestCase):

    @unittest.skip('No views defined in this module')
    def test_all_views(self):
        pass
    
    @unittest.skip('No access rules defined')
    def test_access_rules(self):
        pass

    def test_mdbpool(self):
        from mongodb_backend.mongodb2 import mdbpool
        expect(mdbpool._connection).to(be_none)

        # If we try to access to the connection mongodb connects
        expect(mdbpool.connection).to_not(be_none)

    def test_default_mongodb_name(self):
        from mongodb_backend.mongodb2 import mdbpool
        expect(self.openerp.config.options).to_not(have_keys(
            'mongodb_name',
            'mongodb_port',
            'mongodb_host',
            'mongodb_user',
            'mongodb_pass'
        ))
        # After accessing to getting object variables are defined
        db = mdbpool.get_db()
        expect(self.openerp.config.options).to(have_keys(
            mongodb_name=self.database
        ))


class MongoDBORMTests(testing.MongoDBTestCase):

    def setUp(self):
        self.txn = Transaction().start(self.database)

    def tearDown(self):
        self.txn.stop()

    def create_model(self):
        cursor = self.txn.cursor
        MongoModelTest()
        osv.class_pool[MongoModelTest._name].createInstance(
            self.openerp.pool, 'mongodb_backend', cursor
        )
        mmt_obj = self.openerp.pool.get(MongoModelTest._name)
        mmt_obj._auto_init(cursor)

    def test_name_get(self):
        self.create_model()
        cursor = self.txn.cursor
        uid = self.txn.user
        mmt_obj = self.openerp.pool.get(MongoModelTest._name)
        mmt_id = mmt_obj.create(cursor, uid, {
            'name': 'Foo',
            'other_name': 'Bar',
            'boolean_field': True
        })

        result = mmt_obj.name_get(cursor, uid, [mmt_id])
        self.assertListEqual(
            result,
            [(mmt_id, 'Foo')]
        )

        # Changing the rec_name should use other field
        MongoModelTest._rec_name = 'other_name'
        result = mmt_obj.name_get(cursor, uid, [mmt_id])
        self.assertListEqual(
            result,
            [(mmt_id, 'Bar')]
        )

    def test_boolean(self):
        self.create_model()
        cursor = self.txn.cursor
        uid = self.txn.user
        mmt_obj = self.openerp.pool.get(MongoModelTest._name)
        # Create test
        mmt_id = mmt_obj.create(cursor, uid, {
            'name': 'Foo',
            'other_name': 'Bar',
            'boolean_field': True
        })

        readed_value = mmt_obj.read(cursor, uid, mmt_id, ['boolean_field'])['boolean_field']
        expect(readed_value).to(equal(True))

        # write/search "True"
        for value in [True, 1, '1', [1, 2, ]]:
            mmt_obj.write(cursor, uid, [mmt_id], {'boolean_field': value})

            # read
            readed_value = mmt_obj.read(cursor, uid, mmt_id, ['boolean_field'])['boolean_field']
            expect(readed_value).to(equal(True))

            # search Boolean
            m_ids = mmt_obj.search(cursor, uid, [('boolean_field', '=', True)])
            expect(len(m_ids)).to(be_above(0))

            m_ids = mmt_obj.search(cursor, uid, [('boolean_field', '=', False)])
            expect(len(m_ids)).to(equal(0))

        # write/search "False"
        for value in [False, 0, []]:
            mmt_obj.write(cursor, uid, [mmt_id], {'boolean_field': value})

            # read
            readed_value = mmt_obj.read(cursor, uid, mmt_id, ['boolean_field'])['boolean_field']
            expect(readed_value).to(equal(False))

            # search Boolean
            m_ids = mmt_obj.search(cursor, uid, [('boolean_field', '=', True)])
            expect(len(m_ids)).to(equal(0))

            m_ids = mmt_obj.search(cursor, uid, [('boolean_field', '=', False)])
            expect(len(m_ids)).to(be_above(0))