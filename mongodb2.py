# -*- encoding: utf-8 -*-
##############################################################################
#
#    OpenERP - MongoDB backend
#    Copyright (C) 2011 Joan M. Grande
#    Thanks to Sharoon Thomas for the operator mapping code
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
##############################################################################

import tools
from pymongo import MongoClient, MongoReplicaSetClient
from pymongo.errors import AutoReconnect
from pymongo.read_preferences import ReadPreference
import re
import netsvc
from osv.orm import except_orm
from time import sleep


logger = netsvc.Logger()


class MDBConn(object):

    OPERATOR_MAPPING = {
        '=': lambda l1, l3: {l1: l3},
        '!=': lambda l1, l3: {l1: {'$ne': l3}},
        '<=': lambda l1, l3: {l1: {'$lte': l3}},
        '>=': lambda l1, l3: {l1: {'$gte': l3}},
        '<': lambda l1, l3: {l1: {'$lt': l3}},
        '>': lambda l1, l3: {l1: {'$gt': l3}},

        'in': lambda l1, l3: {l1: {'$in': l3}},
        'not in': lambda l1, l3: {l1: {'$nin': l3}},

        'like': lambda l1, l3: {l1: {
            '$regex': re.compile(l3.replace('%', '.*'))}},
        'not like': lambda l1, l3: {l1: {
        '$not': re.compile('%s' % l3.replace('%', '.*'))}},
        'ilike': lambda l1, l3: {l1: re.compile(l3.replace('%', '.*'), re.I)},
        'not ilike': lambda l1, l3: {l1: {
        '$not': re.compile(l3.replace('%', '.*'), re.I)}},
        }

    def translate_domain(self, domain):
        """Translate an OpenERP domain object to a corresponding
        MongoDB domain

        >>> translate_domain([('name', '=', 'ol')])
        {'name': 'ol'}
        >>> translate_domain([('name', '!=', 'ol')])
        {'name': {'$ne': 'ol'}}

        >>> translate_domain([('name', 'like', 'ol%')])
        {'name': {'$regex': <_sre.SRE_Pattern object at 0x...>}}
        >>> translate_domain([('name', 'not like', '%ol%')])
        {'name': {'$not': <_sre.SRE_Pattern object at 0x...>}}
        >>> translate_domain([('name', 'ilike', '%ol%')])
        {'name': <_sre.SRE_Pattern object at 0x...>}
        >>> translate_domain([('name', 'not ilike', '%ol%')])
        {'name': {'$not': <_sre.SRE_Pattern object at 0x...>}}

        >>> translate_domain([('_id', 'in', [1, 2, 3])])
        {'_id': {'$in': [1, 2, 3]}}
        >>> translate_domain([('_id', 'not in', [1, 2, 3])])
        {'_id': {'$nin': [1, 2, 3]}}
        >>> translate_domain([('_id', '<=', 10)])
        {'_id': {'$lte': 10}}
        >>> translate_domain([('_id', '<', 10)])
        {'_id': {'$lt': 10}}
        >>> translate_domain([('_id', '>=', 10)])
        {'_id': {'$gte': 10}}
        >>> translate_domain([('_id', '>', 10)])
        {'_id': {'$gt': 10}}
        >>> translate_domain([('_id', '>', 10), ('_id', '<', 15)])
        {'_id': {'$gt': 10, '$lt': 15}}
        >>> translate_domain([('_id', '>', 10),
                              ('_id', '<', 15),
                              ('name', 'ilike', '%ol%')])
        {'_id': {'$gt': 10, '$lt': 15},
         'name': <_sre.SRE_Pattern object at 0x...>}
        """
        new_domain = {}
        for field, operator, value in domain:
            clause = self.OPERATOR_MAPPING[operator](field, value)
            if field in new_domain.keys():
                new_domain[field].update(clause[field])
            else:
                new_domain.update(clause)
        return new_domain

    @property
    def uri(self):
        """ Mongo uri calculation with backward compatibility prior to 0.4v
        """
        def_db = tools.config.get('db_name', 'openerp')
        tools.config['mongodb_name'] = tools.config.get('mongodb_name',
                                                        def_db)
        tools.config['mongodb_port'] = tools.config.get('mongodb_port', '')
        tools.config['mongodb_host'] = tools.config.get('mongodb_host', '')
        tools.config['mongodb_user'] = tools.config.get('mongodb_user', '')
        tools.config['mongodb_pass'] = tools.config.get('mongodb_pass', '')
        tools.config['mongodb_uri'] = tools.config.get(  # Default
            'mongodb_uri', 'mongodb://localhost:27017/'
        )

        """
            MONGODB-CR  - mongo 2.4, 2.6 - defecto para mantener compatibilidad
            SCRAM-SHA-1 - mongo 3.x
        """
        tools.config['mongodb_auth'] = tools.config.get('mongodb_auth',
                                                        'MONGODB-CR')

        uri = tools.config['mongodb_uri']  # with replicaset must use uri
        if not tools.config.get('mongodb_replicaset', False):
            if tools.config['mongodb_user']:
                # Auth
                uri_tmpl = 'mongodb://%s:%s@%s:%s/%s?authMechanism=%s'
                uri = uri_tmpl % (tools.config['mongodb_user'],
                                  tools.config['mongodb_pass'],
                                  tools.config['mongodb_host'],
                                  tools.config['mongodb_port'],
                                  tools.config['mongodb_name'],
                                  tools.config['mongodb_auth'])
            elif tools.config['mongodb_host'] and tools.config['mongodb_port']:
                # No auth
                uri_tmpl = 'mongodb://%s:%s/'
                uri = uri_tmpl % (tools.config['mongodb_host'],
                                  int(tools.config['mongodb_port']))
        return uri

    def mongo_connect(self):
        '''Connects to mongo'''
        try:
            tools.config['mongodb_replicaset'] = tools.config.get(
                'mongodb_replicaset', False
            )
            mongo_client = MongoClient
            kwargs = {}
            if tools.config['mongodb_replicaset']:
                kwargs.update({'replicaSet': tools.config['mongodb_replicaset'],
                               'read_preference': ReadPreference.PRIMARY_PREFERRED})
                mongo_client = MongoReplicaSetClient

            connection = mongo_client(self.uri, **kwargs)
        except Exception, e:
            raise except_orm('MongoDB connection error', e)
        return connection

    def __init__(self):
        self._connection = None

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self.mongo_connect()
        return self._connection

    def get_collection(self, collection):

        try:
            db = self.connection[tools.config['mongodb_name']]
            collection = db[collection]

        except AutoReconnect as ar_e:
            max_tries = 5
            count = 0
            while count < max_tries:
                try:
                    logger.notifyChannel('MongoDB', netsvc.LOG_WARNING,
                                 'trying to reconnect...')
                    con = self.mongo_connect()

                    db = con[tools.config['mongodb_name']]
                    collection = db[collection]
                    break
                except AutoReconnect:
                    count += 1
                    sleep(0.5)
            if count == 4:
                raise except_orm('MongoDB connection error', ar_e)
        except Exception, e:
            raise except_orm('MongoDB connection error', e)

        return collection

    def get_db(self):

        try:
            db = self.connection[tools.config['mongodb_name']]
        except AutoReconnect:
            max_tries = 5
            count = 0
            while count < max_tries:
                try:
                    logger.notifyChannel('MongoDB', netsvc.LOG_WARNING,
                                 'WARNING: MongoDB trying to reconnect...')
                    con = self.mongo_connect()

                    db = con[tools.config['mongodb_name']]
                    break
                except AutoReconnect:
                    count += 1
                    sleep(0.5)
        except Exception, e:
            raise except_orm('MongoDB connection error', e)

        return db

    def end_request(self):
        return self.connection.end_request()

mdbpool = MDBConn()
