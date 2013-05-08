# -*- coding: utf-8 -*-

from osv.fields import char
from tools import human_size

import pymongo
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
        self.versioning = False
        super(gridfs, self).__init__(string=string, size=24, widget='binary',
                                     **args)

    def get_oids(self, cursor, obj, ids, name):
        cursor.execute("select id, " + name + " from " + obj._table +
                       " where id  in %s", (tuple(ids), ))
        res = dict([(x[0], x[1]) for x in cursor.fetchall()])
        return res

    def get_filename(self, obj, rid, name):
        return '%s/%s_%s' % (obj._table, rid, name)

    def set(self, cursor, obj, rid, name, value, user=None, context=None):
        # TODO: Store some more metadata. File name, author, etc.
        db = mdbpool.get_db()
        fs = gfs.GridFS(db, collection='fs')
        for rid, oid in self.get_oids(cursor, obj, [rid], name).items():
            filename = self.get_filename(obj, rid, name)
            if oid and fs.exists(ObjectId(oid)) and not self.versioning:
                fs.delete(ObjectId(oid))
            if value:
                _id = fs.put(value, filename=filename)
                value = str(_id)
            if not value and self.versioning:
                fs.delete(ObjectId(oid))
                res = db.fs.files.find(
                    {'filename': filename},
                    {'uploadDate': True, '_id': True}
                ).sort('filename', pymongo.DESCENDING).limit(1)
                if res.count():
                    value = str(res[0]['_id'])
            return super(gridfs, self).set(cursor, obj, rid, name, value, user,
                                           context)

    def get(self, cursor, obj, ids, name, user=None, offset=0, context=None,
            values=None):
        if not context:
            context = {}
        db = mdbpool.get_db()
        fs = gfs.GridFS(db, collection='fs')
        res = self.get_oids(cursor, obj, ids, name)
        for rid, oid in res.items():
            filename = self.get_filename(obj, rid, name)
            if oid:
                oid = ObjectId(oid)
                val = fs.get(oid).read()
                if context.get('bin_size', False) and val:
                    version = db.fs.files.find(
                        {'filename': filename},
                        {'uploadDate': True, '_id': False}
                    ).count()
                    res[rid] = '%s - v%s' % (human_size(val), version)
                else:
                    res[rid] = val
            else:
                res[rid] = False
        return res
