require "rubygems"
require "xmpp4r"

require "xmpp4r/pubsub"
require "xmpp4r/pubsub/helper/servicehelper.rb"
require "xmpp4r/pubsub/helper/nodebrowser.rb"
require "xmpp4r/pubsub/helper/nodehelper.rb"

require "config.rb"

include Jabber
Jabber::debug = true

puts 'Create leaf node'

if ARGV.size() == 0:
	puts 'usage: get-config <node_name>'
	exit
end


service = IDAVOLL_SCRIPT_SERVICE
jid = IDAVOLL_SCRIPT_JID
password = IDAVOLL_SCRIPT_PASSWORD

client = Client.new(JID.new(jid))
client.connect
client.auth(password)

client.send(Jabber::Presence.new.set_type(:available))
pubsub = PubSub::ServiceHelper.new(client, service)

nodename = ARGV[0]

ret = pubsub.get_config_from(nodename)
if ret:
	puts 'Configure:'
	puts ret
end


