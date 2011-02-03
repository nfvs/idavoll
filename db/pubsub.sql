/*CREATE LANGUAGE plpgsql;*/

CREATE USER pubsub WITH PASSWORD 'pubsub';

CREATE TABLE entities (
    entity_id serial PRIMARY KEY,
    jid text NOT NULL UNIQUE
);

CREATE TABLE nodes (
    node_id serial PRIMARY KEY,
    node text NOT NULL UNIQUE,
    node_type text NOT NULL DEFAULT 'leaf'
        CHECK (node_type IN ('leaf', 'collection')),
/*	collection text DEFAULT NULL,
	path ltree, */

	collection integer DEFAULT NULL, /* parent */
/*	lft integer NOT NULL,
	rgt integer NOT NULL, */

    persist_items boolean,
    deliver_payloads boolean NOT NULL DEFAULT TRUE,
    send_last_published_item text NOT NULL DEFAULT 'on_sub'
        CHECK (send_last_published_item IN ('never', 'on_sub'))
);

/*
CREATE INDEX nodes_path_gist_idx ON nodes USING gist(path);
CREATE INDEX nodes_path_idx ON nodes USING btree(path);
*/


/*ALTER TABLE nodes ADD CONSTRAINT pk_nodes_id PRIMARY KEY (node_id);
CREATE INDEX ix_nodes_lft ON nodes (lft);
CREATE INDEX ix_nodes_rgt ON nodes (rgt);*/
CREATE INDEX ix_nodes_id ON nodes (node_id);
CREATE INDEX ix_nodes_parent ON nodes (collection);


INSERT INTO nodes (node_id, node, node_type) values (0, '', 'collection');


CREATE TABLE affiliations (
    affiliation_id serial PRIMARY KEY,
    entity_id integer NOT NULL REFERENCES entities ON DELETE CASCADE,
    node_id integer NOT NULL references nodes ON DELETE CASCADE,
    affiliation text NOT NULL
        CHECK (affiliation IN ('outcast', 'publisher', 'owner')),
    UNIQUE (entity_id, node_id)
);

CREATE TABLE subscriptions (
    subscription_id serial PRIMARY KEY,
    entity_id integer NOT NULL REFERENCES entities ON DELETE CASCADE,
    resource text,
    node_id integer NOT NULL REFERENCES nodes ON delete CASCADE,
    state text NOT NULL DEFAULT 'subscribed'
    	CHECK (state IN ('subscribed', 'pending', 'unconfigured')),
    subscription_type text
    	CHECK (subscription_type IN (NULL, 'items', 'nodes')),
    subscription_depth text
    	CHECK (subscription_depth IN (NULL, '1', 'all')),
    UNIQUE (entity_id, resource, node_id));

CREATE TABLE items (
    item_id serial PRIMARY KEY,
    node_id integer NOT NULL REFERENCES nodes ON DELETE CASCADE,
    item text NOT NULL,
    publisher text NOT NULL,
    data text,
    date timestamp with time zone NOT NULL DEFAULT now(),
    UNIQUE (node_id, item)
);

GRANT ALL PRIVILEGES ON affiliations TO pubsub;
GRANT ALL PRIVILEGES ON affiliations_affiliation_id_seq TO pubsub;
GRANT ALL PRIVILEGES ON entities TO pubsub;
GRANT ALL PRIVILEGES ON entities_entity_id_seq TO pubsub;
GRANT ALL PRIVILEGES ON items TO pubsub;
GRANT ALL PRIVILEGES ON items_item_id_seq TO pubsub;
GRANT ALL PRIVILEGES ON nodes TO pubsub;
GRANT ALL PRIVILEGES ON nodes_node_id_seq TO pubsub;
GRANT ALL PRIVILEGES ON subscriptions TO pubsub;
GRANT ALL PRIVILEGES ON subscriptions_subscription_id_seq TO pubsub;
