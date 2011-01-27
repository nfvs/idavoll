require "rubygems"
require "xmpp4r"

require "xmpp4r/pubsub"
require "xmpp4r/pubsub/helper/servicehelper.rb"
require "xmpp4r/pubsub/helper/nodebrowser.rb"
require "xmpp4r/pubsub/helper/nodehelper.rb"

include Jabber
Jabber::debug = true


require 'config.rb'

service = IDAVOLL_SCRIPT_SERVICE
jid = IDAVOLL_SCRIPT_JID
password = IDAVOLL_SCRIPT_PASSWORD

client = Client.new(JID.new(jid))
client.connect
client.auth(password)

pubsub = PubSub::ServiceHelper.new(client, service)

