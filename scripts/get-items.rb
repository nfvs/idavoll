require "rubygems"
require "xmpp4r"
require "pp"

require "xmpp4r/pubsub"
require "xmpp4r/pubsub/helper/servicehelper.rb"
require "xmpp4r/pubsub/helper/nodebrowser.rb"
require "xmpp4r/pubsub/helper/nodehelper.rb"

require "config.rb"

include Jabber
Jabber::debug = true

puts 'publish item'

if ARGV.size() != 2:
	puts 'usage: get-items <node_name> <item count>'
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
count = ARGV[1]


ret = pubsub.get_items_from(nodename, count=count)
if ret:
  puts 'Items:'
  ret.each do |k, v|
    puts '  id: %s' % k
    puts '  xml: %s' % v
  end
end




