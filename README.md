# Idavoll 0.9.1

This is a fork of Ralph Meijer's Idavoll (<https://github.com/ralphm/idavoll>).  
View README for more information.

# New Features

* Support for [XEP-0248](http://xmpp.org/extensions/xep-0248.html) (PubSub Collection Nodes)
* Support for [XEP-0030](http://xmpp.org/extensions/xep-0030.html) (Service Discovery)
* Retrieving all subscriptions for a given node
* Managing node configuration
* Managing subscription options
* Managing node affiliations
* PostgreSQL+CouchDB hybrid storage engine (stores Items in CouchDB, and everything else in PostreSQL)


# Install

## Requirements

* Twisted >= 8.0.1
    * Twisted Core
	* Twisted Conch (for idavoll-http)
	* Twisted Web (for idavoll-http)
	* Twisted Web2 (for idavoll-http)
	* Twisted Words
* uuid.py (Python 2.5 std. lib. or http://pypi.python.org/pypi/uuid)
* Wokkel >= 0.5.0 (http://github.com/nfvs/wokkel)
* simplejson (for idavoll-http)
* A Jabber server that supports the component protocol (XEP-0114)

For the PostgreSQL backend:
* PostgreSQL
* pyPgSQL

For the CouchDB backend
* CouchDB >=1.0.1

## Installation

    python setup.py build
    python setup.py install

# Configuration

The easiest way to start Idavoll is to use a .tac configuration file, as given in doc/examples/idavoll.tac.

## Common configuration
    'jid': JID('<PubSub JID to be used by this component>')
	'secret': '<secret used to communicate with the XMPP server'
	'rhost': '<hostname of the XMPP server component interface>'
	'rport': <port number of the XMPP server component interface>
	'backend': '<backend to be used; e.g. pgsql, pgsq_couchdb>'

## PostgreSQL configuration
    'dbhost': '<hostname of the PostgreSQL database>'
    'dbport': '<port number of the PostgreSQL database>'
    'dbname': '<name of the PostgreSQL database>'
    'dbuser': '<username to access the PostgreSQL database>'
    'dbpass': '<password of the username to access the PostgreSQL database>'

## CouchDB configuration
    'cdbhost': '<hostname of the CouchDB database>'
	'cdbport': '<port number of the CouchDB database>'
	'cdbname': '<name of the CouchDB database>'

# Running

To run idavoll using a configuration file config.tac, use:
    twistd -y config.tac

# Contact

Nuno Santos (<nunofvsantos@gmail.com>)  
Ralph Meijer (<ralphm@ik.nu>)
