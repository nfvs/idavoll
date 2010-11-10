# XMPP Server Configuration
XMPP_HOSTNAME=localhost
XMPP_PORT=5275
XMPP_SECRET=secret
BACKEND=couchdb

# comment out to use daemon
IDAVOLL_ARGS=-n

if [ -f twistd.pid ]
then
        echo "Stopping previous Idavoll deamon..."
        kill $(cat twistd.pid)
        echo "Stopped."
fi

twistd $IDAVOLL_ARGS idavoll --rhost=$XMPP_HOSTNAME --rport=$XMPP_PORT --secret=$XMPP_SECRET --backend=$BACKEND
echo "Lanching Idavoll. XMPP Server: $XMPP_HOSTNAME:$XMPP_PORT (backend=$BACKEND)"
