#!/usr/bin/env bash
dropdb pubsub
createdb pubsub
psql pubsub < /usr/share/postgresql/8.4/contrib/ltree.sql 
psql pubsub < pubsub.sql

#psql pubsub_test < /usr/share/postgresql/8.4/contrib/ltree.sql 
#psql pubsub_test < pubsub.sql
