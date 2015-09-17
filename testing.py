from destral import testing
from mongodb_backend.mongodb2 import mdbpool


class MongoDBTestCase(testing.OOTestCase):

    def tearDown(self):
        super(MongoDBTestCase, self).tearDown()
        if self.drop_database:
            mongodb_name = self.openerp.config['mongodb_name']
            mdbpool.connection.drop_database(mongodb_name)
        # Treat each test as new codebase
        mdbpool._connection = None
