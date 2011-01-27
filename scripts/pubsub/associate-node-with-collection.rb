require "rubygems"
require "xmpp4r"

require "xmpp4r/pubsub"
require "xmpp4r/pubsub/helper/servicehelper.rb"
require "xmpp4r/pubsub/helper/nodebrowser.rb"
require "xmpp4r/pubsub/helper/nodehelper.rb"

include Jabber
Jabber::debug = true

puts 'Create leaf node'

if ARGV.size() == 0:
	puts 'usage: associate-node-with-collection <node_name> <collection>'
	exit
end


service = 'pubsub.ubuntu'
jid = 'nfvs@ubuntu/script'

password = 'nfvs'
client = Client.new(JID.new(jid))
client.connect
client.auth(password)

client.send(Jabber::Presence.new.set_type(:available))
pubsub = PubSub::ServiceHelper.new(client, service)

nodename = ARGV[0]
config = {}
if ARGV[1]:
	config['pubsub#collection'] = ARGV[1]
end


opt = Jabber::PubSub::NodeConfig.new(node=nodename, config=config)


ret = pubsub.set_config_for(nodename, opt)
if ret:
	puts 'config success'
end

