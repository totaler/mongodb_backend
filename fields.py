# -*- coding: utf-8 -*-

from osv.fields import char
from tools import human_size

import gridfs as gfs
from mongodb2 import mdbpool
from bson.objectid import ObjectId


class gridfs(char):
    """GridFS filesystem PostgreSQL <-> MongoDB

    We will save ObjectId string into PostgreSQL database
    """
    _classic_read = False
    _classic_write = False
    _type = 'char'

    def __init__(self, string, **args):
        super(gridfs, self).__init__(string=string, size=24, widget='binary',
                                     **args)

    def get_oids(self, cursor, obj, ids, name):
        cursor.execute("select id, " + name + " from " + obj._table +
                       " where id  in %s", (tuple(ids), ))
        res = dict([(x[0], x[1]) for x in cursor.fetchall()])
        return res

    def set(self, cursor, obj, rid, name, value, user=None, context=None):
        # TODO: Store some more metadata. File name, author, etc.
        fs = gfs.GridFS(mdbpool.get_db(), collection='fs')
        # Maybe support file versioning using obj._table + _rid?
        for rid, oid in self.get_oids(cursor, obj, [rid], name).items():
            if oid and fs.exists(ObjectId(oid)):
                fs.delete(ObjectId(oid))
            if value:
                _id = fs.put(value)
                value = str(_id)
            return super(gridfs, self).set(cursor, obj, rid, name, value, user,
                                           context)

    def get(self, cursor, obj, ids, name, user=None, offset=0, context=None,
            values=None):
        if not context:
            context= {}
        fs = gfs.GridFS(mdbpool.get_db(), collection='fs')
        res = self.get_oids(cursor, obj, ids, name)
        for rid, oid in res.items():
            if oid:
                oid = ObjectId(oid)
                val = fs.get(oid).read()
                if context.get('bin_size', False) and val:
                    res[rid] = human_size(val)
                else:
                    res[rid] = val
            else:
                res[rid] = False
        return res
