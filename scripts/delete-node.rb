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
	puts 'usage: delete-node <node_name>'
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


ARGV.each do|node_name|
	pubsub.delete_node(node_name)
end

