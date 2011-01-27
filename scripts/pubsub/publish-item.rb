require "rubygems"
require "xmpp4r"

require "xmpp4r/pubsub"
require "xmpp4r/pubsub/helper/servicehelper.rb"
require "xmpp4r/pubsub/helper/nodebrowser.rb"
require "xmpp4r/pubsub/helper/nodehelper.rb"

require "config.rb"

include Jabber
Jabber::debug = true

puts 'publish item'

if ARGV.size() != 1:
	puts 'usage: get-config <node_name>'
	exit
end


service = IDAVOLL_SCRIPT_SERVICE
jid = IDAVOLL_SCRIPT_JID
password = IDAVOLL_SCRIPT_PASSWORD

client = Client.new(JID.new(jid))
client.connect
client.auth(password)

#client.send(Jabber::Presence.new.set_type(:available))
pubsub = PubSub::ServiceHelper.new(client, service)

nodename = ARGV[0]


item = Jabber::PubSub::Item.new()

ret = pubsub.publish_item_to(nodename, item)
if ret:
	puts ret
end




