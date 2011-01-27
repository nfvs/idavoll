from couchdbkit import Server

s = Server('http://ubuntu:5984')

s.delete_db('pubsub')
