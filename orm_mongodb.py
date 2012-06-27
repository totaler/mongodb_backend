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

from osv import orm
from osv.orm import except_orm
import netsvc
import re
import pymongo
from datetime import datetime

#mongodb stuff
try:
    from mongodb2 import mdbpool
except ImportError:
    sys.stderr.write("ERROR: Import mongodb module\n")

class orm_mongodb(orm.orm_template):

    _protected = ['read','write','create','default_get','perm_read','unlink',
                  'fields_get','fields_view_get','search','name_get',
                  'distinct_field_get','name_search','copy','import_data',
                  'search_count', 'exists']

    def _auto_init(self, cr, context={}):
        self._field_create(cr, context=context)
        logger = netsvc.Logger()

        db = mdbpool.get_db()        

        #Create the model counters document in order to 
        #have incremental ids the way postgresql does
        collection = db['counters']
        
        if not collection.find({'_id': self._table}).count():
            vals = {'_id': self._table,
                    'counter': 0}
            collection.save(vals)

        collection = db[self._table]
        #Create index for the id field
        collection.ensure_index([('id', pymongo.ASCENDING)],  
                                deprecated_unique=None, 
                                ttl=300, 
                                unique = True)

        if db.error():
            raise except_orm('MongoDB create id field index error', db.error())
        #Update docs with new default values if they do not exist
        def_fields = filter(lambda a: not collection.find(
                                          {a:{'$exists': True}}).count(), 
                                          self._defaults.keys())
        if len(def_fields):
            logger.notifyChannel('orm', netsvc.LOG_DEBUG, 
                                 'setting default value for \
                                  %s of collection %s'% (def_fields, self._table))
            def_values = self.default_get(cr, 1, def_fields)
            collection.update({}, 
                              {'$set': def_values},
                              upsert=False, 
                              manipulate=False, 
                              safe=True,
                              multi=True)

        if db.error():
            raise except_orm('MongoDB update defaults error', db.error()) 

        
    def __init__(self, cr):
        super(orm_mongodb, self).__init__(cr)
        cr.execute('delete from wkf_instance where res_type=%s', (self._name,))

    def get_date_fields(self):
        return [key for key, val in self._columns.iteritems()
                      if val._type in ('date', 'datetime')]

    def get_bool_fields(self):
        return [key for key, val in self._columns.iteritems()
                      if val._type in ('boolean')]
        
    def transform_date_field(self, field, value, action):

        if not value:
            return value
        
        if self._columns[field]._type == 'date':
            date_format = '%Y-%m-%d'
        elif self._columns[field]._type == 'datetime':
            date_format = '%Y-%m-%d %H:%M:%S'
        
        if action == 'read':
            return value.strftime(date_format)
        elif action == 'write':
            #When searching datetime objects, string do not take time
            only_date = re.compile("^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
            if only_date.match(value):
                date_format = '%Y-%m-%d'
            return datetime.strptime(value, date_format)                        

    def read_date_fields(self, fields, vals):
        date_fields = self.get_date_fields()
        date_fields_to_read = list(set(fields) & set(date_fields)) 
        if date_fields_to_read:
            for val in vals:
                for date_field in date_fields_to_read:
                    val[date_field] = self.transform_date_field(date_field,
                                                            val[date_field],
                                                                'read')

    def write_date_fields(self, val):
        date_fields = self.get_date_fields()
        fields = val.keys()
        date_fields_to_write = list(set(fields) & set(date_fields)) 
        if date_fields_to_write:
            for date_field in date_fields_to_write:
                val[date_field] = self.transform_date_field(date_field,
                                                        val[date_field],
                                                            'write')

    def search_trans_fields(self, args):
        date_fields = self.get_date_fields()
        bool_fields = self.get_bool_fields()
        for arg in args:
            if arg[0] in date_fields:
                arg[2] = self.transform_date_field(arg[0],
                                                   arg[2],
                                                   'write')
            if arg[0] in bool_fields:
                arg[2] = bool(arg[2])        

    def read(self, cr, user, ids, fields=None, context=None, load='_classic_read'):

        if not context:
            context = {}
        self.pool.get('ir.model.access').check(cr, user, self._name, 
                                                'read', context=context)
        if not fields:
            fields = self._columns.keys()
        select = ids
        if isinstance(ids, (int, long)):
            select = [ids]
        result = self._read_flat(cr, user, select, fields, context, load)
        
        for r in result:
            for key, v in r.items():
                #remove the '_id' field from the response
                if key == '_id':
                    del r[key]
                    continue
                if v is None:
                    r[key] = False
                else:
                    continue

        if isinstance(ids, (int, long)):
            return result and result[0] or False
        return result

    def _read_flat(self, cr, user, ids, fields_to_read, context=None, load='_classic_read'):

        collection = mdbpool.get_collection(self._table)

        if not context:
            context = {}
        if not ids:
            return []

        if fields_to_read == None:
            fields_to_read = self._columns.keys()

        # All non inherited fields for which the attribute whose name is in load is True
        fields_pre = [f for f in fields_to_read if
                           f == self.CONCURRENCY_CHECK_FIELD
                        or (f in self._columns and getattr(self._columns[f], 
                                                           '_classic_write'))
                     ]

        res = []
        if len(fields_pre):
            order = self._compute_order(cr, user)
            mongo_cr = collection.find({'id': {'$in': ids}},
                                       fields_pre + ['id'],
                                       sort=order)
            res = [x for x in mongo_cr]
        else:
            res = map(lambda x: {'id': x}, ids)
        #Post process date and datetime fields
        self.read_date_fields(fields_to_read, res)
        
        return res

    def write(self, cr, user, ids, vals, context=None):

        db = mdbpool.get_db()
        collection = mdbpool.get_collection(self._table)
        vals = vals.copy()

        if not ids:
            return True
        
        #Pre process date and datetime fields
        self.write_date_fields(vals)

        #Log access
        vals.update({'write_uid': user, 
                     'write_date': datetime.now(),
                    })

        #bulk update with modifiers, and safe mode
        collection.update({'id': { '$in' : ids }}, 
                          {'$set': vals},
                          False, False, True, True)

        if db.error():
            raise except_orm('MongoDB update error', db.error())   
       
        return True

    def create(self, cr, user, vals, context=None):

        collection = mdbpool.get_collection(self._table)
        vals = vals.copy()
        
        if not context:
            context = {}
        self.pool.get('ir.model.access').check(cr, user, self._name, 
                                               'create', context=context)

        #Default values
        default = [f for f in self._columns.keys()
                     if f not in vals]

        if len(default):
            default_values = self.default_get(cr, user, default, context)
            vals.update(default_values)

        #Add incremental id to store vals
        counter = mdbpool.get_collection('counters').find_and_modify(
                    {'_id': self._table}, 
                    {'$inc': {'counter': 1}})
        vals.update({'id': counter['counter']})
        #Pre proces date fields
        self.write_date_fields(vals)
        #Log access
        vals.update({'create_uid': user, 
                     'create_date': datetime.now(),
                    })
        
        #Effectively create the record
        collection.insert(vals)
        
        return vals['id']    

    def _compute_order(self, cr, user, order=None, context=None):
        #Parse the order of the object to addapt it to MongoDB

        if not order:
            order = self._order
        
        mongo_order = order.split(',')
        #If we only have one order field
        #it can contain asc or desc 
        #Otherwise is not allowed
        if len(mongo_order) == 1:
            order_desc = re.compile('^(([a-z0-9_]+|"[a-z0-9_]+")( *desc)+( *, *|))+$', re.I)
            if order_desc.match(mongo_order[0].strip()):
                return [(mongo_order[0].partition(' ')[0].strip(),
                        pymongo.DESCENDING)]
            else:
                return [(mongo_order[0].partition(' ')[0].strip(),
                        pymongo.ASCENDING)]
        else:
            res = []
            regex_order_mongo = re.compile('^(([a-z0-9_]+|"[a-z0-9_]+")( *desc| *asc)+( *, *|))+$', re.I)
            for field in mongo_order:
                if regex_order_mongo.match(field.strip()):
                    raise except_orm(_('Error'),
                        _('Bad order declaration for model %s')%(self._name))
                else:                  
                    res.append((field.strip(), pymongo.ASCENDING))
        return res

    def search(self, cr, user, args, offset=0, limit=0, order=None,
            context=None, count=False):
        #Make a copy of args for working
        #Domain has to be list of lists
        tmp_args = [isinstance(arg, tuple) and list(arg) or arg for arg in args]
        collection = mdbpool.get_collection(self._table)
        self.search_trans_fields(tmp_args)

        new_args = mdbpool.translate_domain(tmp_args)
        if not context:
            context = {}
        self.pool.get('ir.model.access').check(cr, user, 
                        self._name, 'read', context=context)

        if count:
            return collection.find(
                    new_args,
                    {'id':1}, 
                    skip=int(offset), 
                    limit=int(limit), 
                    timeout=True, 
                    snapshot=False, 
                    tailable=False, 
                    sort=self._compute_order(cr, user, order)).count()
        
        mongo_cr = collection.find(
                    new_args,
                    {'id':1}, 
                    skip=int(offset), 
                    limit=int(limit), 
                    timeout=True, 
                    snapshot=False, 
                    tailable=False, 
                    sort=self._compute_order(cr, user, order))

        res = [x['id'] for x in mongo_cr]

        return res

    def unlink(self, cr, uid, ids, context=None):

        db = mdbpool.get_db()
        collection = mdbpool.get_collection(self._table)

        if not ids:
            return True
        if isinstance(ids, (int, long)):
            ids = [ids]

        self.pool.get('ir.model.access').check(cr, uid, self._name, 
                                               'unlink', context=context)
        
        #Remove with safe mode
        collection.remove({'id': {'$in': ids}}, True)

        if db.error():
            raise except_orm('MongoDB unlink error', db.error())        

        return True

    def _check_removed_columns(self, cr, log=False):
        # nothing to check in schema free...
        pass

    def perm_read(self, cr, user, ids, context={}, details=True):
        
        if not ids:
            return []
        
        if isinstance(ids, (int, long)):
            ids = [ids]
        
        collection = mdbpool.get_collection(self._table)

        fields = ['id', 'create_uid', 'create_date', 
                  'write_uid', 'write_date']

        res = []
        mongo_cr = collection.find({'id': {'$in': ids}}, fields)
        res = [x for x in mongo_cr]
        for doc in res:
            docfields = doc.keys()
            for field in fields:
                if field not in docfields:
                    doc[field] = False                    
                if field in ['create_date', 'write_date']\
                    and doc[field]:
                    doc[field] = doc[field].strftime('%Y-%m-%d %H:%M:%S')
                if field in ['create_uid', 'write_uid']\
                    and doc[field]:
                    doc[field] = self.pool.get('res.users').name_get(cr, user, [doc[field]])[0]
            del doc['_id']

        return res

    def default_get(self, cr, uid, fields_list, context={}):
        
        value = {}
        
        # get the default values defined in the object
        for f in fields_list:
            if f in self._defaults:
                value[f] = self._defaults[f](self, cr, uid, context)
            
        return value
        

