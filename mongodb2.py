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
from pymongo import Connection
from pymongo.errors import AutoReconnect
import re
import netsvc
from osv.orm import except_orm
from time import sleep


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

    def mongo_connect(self):
        '''Connects to mongo'''
        try:
            if tools.config['mongodb_user']:
                uri_tmpl = 'mongodb://%s:%s@%s:%s/%s'
                uri = uri_tmpl % (tools.config['mongodb_user'],
                                  tools.config['mongodb_pass'],
                                  tools.config['mongodb_host'],
                                  tools.config['mongodb_port'],
                                  tools.config['mongodb_name'])
                connection = Connection(uri)
            else:
                connection = Connection(tools.config['mongodb_host'],
                                        int(tools.config['mongodb_port']))
        except Exception, e:
            raise except_orm('MongoDB connection error', e)
        return connection

    def __init__(self):
        def_db = tools.config.get('db_name', 'openerp')
        tools.config['mongodb_name'] = tools.config.get('mongodb_name', def_db)
        tools.config['mongodb_port'] = tools.config.get('mongodb_port', 27017)
        tools.config['mongodb_host'] = tools.config.get('mongodb_host',
                                                        'localhost')
        tools.config['mongodb_user'] = tools.config.get('mongodb_user', '')
        tools.config['mongodb_pass'] = tools.config.get('mongodb_pass', '')

        self.connection = self.mongo_connect()

    def get_collection(self, collection):

        try:
            db = self.connection[tools.config['mongodb_name']]
            collection = db[collection]

        except AutoReconnect:
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
                raise except_orm('MongoDB connection error', e)
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
