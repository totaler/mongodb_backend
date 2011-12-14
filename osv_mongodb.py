# -*- encoding: utf-8 -*-
##############################################################################
#
#    OpenERP - MongoDB backend  
#    Copyright (C) 2011 Joan M. Grande <totaler@gmail.com>
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

from osv.osv import module_class_list, module_list, class_pool
import orm_mongodb

class osv_mongodb(orm_mongodb.orm_mongodb):
    #__metaclass__ = inheritor
    def __new__(cls):
        module = str(cls)[6:]
        module = module[:len(module)-1]
        module = module.split('.')[0][2:]
        if not hasattr(cls, '_module'):
            cls._module = module
        module_class_list.setdefault(cls._module, []).append(cls)
        class_pool[cls._name] = cls
        if module not in module_list:
            module_list.append(cls._module)
        return None

    def createInstance(cls, pool, module, cr):
        obj = object.__new__(cls)
        obj.__init__(pool, cr)
        return obj
    createInstance = classmethod(createInstance)

    def __init__(self, pool, cr):
        pool.add(self._name, self)
        self.pool = pool
        orm_mongodb.orm_mongodb.__init__(self, cr)
