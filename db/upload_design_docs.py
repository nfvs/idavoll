#!/usr/bin/env python
import sys

from couchdbkit import Server

if len(sys.argv) <= 3:
    print 'Usage: %s <couchdb-url> <couchdb-db> <path-to-couchdb-design-docs>' % sys.argv[0]
    print
    exit(-1)

url = sys.argv[1]
db = sys.argv[2]
p = sys.argv[3]

print
print 'Uploading design docs'
print 'from \t folder = "%s"' % p
print 'to \t url = "%s", db = "%s"' % (url, db)
print 'Ok? (y/n)'
a = raw_input()

if a != 'y' and a != 'Y':
    exit(-1)


# upload design docs
from couchdbkit.loaders import FileSystemDocsLoader
loader = FileSystemDocsLoader(p)
server = Server(url)
cdb = server.get_or_create_db(db)
loader.sync(cdb)
