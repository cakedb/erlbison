
import datetime
import re

from bson import BSON, binary, objectid, code, timestamp, min_key, max_key

document = {
            "some_double" : 87363.343425,
            "some_string" : "HelloWorld",
            "some_document" : {"nested":"document"},
            "some_array" : [1, True, "a", None],
            "some_binary" : binary.Binary("HelloWorld"),
            "some_ObjectId" : objectid.ObjectId('5130d8c37603e11f843f9c05'),
            "some_bool" : True,
            "some_datetime" : datetime.datetime(2013,3,1,12,30,30),
            "some_null" : None,
            "some_regex" : re.compile("a*b?c\?"),
            "some_other_regex" : re.compile("(?:\+?1\s*(?:[.-]\s*)?)"),
            "some_jscode" : code.Code('document.write("Hello World!")'),
            "some_jscodews" : code.Code('document.write("Hello World!")', {"scopevar" : "scopevalue"}),
            "some_int32" : 1,
            "some_timestamp" : timestamp.Timestamp(1361851945,0),
            "some_int64" : long(3000000000),
            "some_other_int64" : long(-20),
            "some_minkey" : min_key.MinKey(),
            "some_maxkey" : max_key.MaxKey(),
           }

bson_file = open("test/test.bson",'wb')
bson_file.write(BSON.encode(document))
bson_file.close()

