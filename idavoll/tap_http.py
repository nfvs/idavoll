# Copyright (c) 2003-2008 Ralph Meijer
# See LICENSE for details.

from twisted.application import internet, strports
from twisted.conch import manhole, manhole_ssh
from twisted.cred import portal, checkers
from twisted.web2 import channel, resource, server

from idavoll import gateway, tap
from idavoll.gateway import RemoteSubscriptionService

class Options(tap.Options):
    optParameters = [
            ('webport', None, '8086', 'Web port'),
    ]


def getManholeFactory(namespace, **passwords):
    def getManHole(_):
        return manhole.Manhole(namespace)

    realm = manhole_ssh.TerminalRealm()
    realm.chainedProtocolFactory.protocolFactory = getManHole
    p = portal.Portal(realm)
    p.registerChecker(
            checkers.InMemoryUsernamePasswordDatabaseDontUse(**passwords))
    f = manhole_ssh.ConchFactory(p)
    return f


def makeService(config):
    s = tap.makeService(config)

    bs = s.getServiceNamed('backend')
    cs = s.getServiceNamed('component')

    # Set up XMPP service for subscribing to remote nodes

    ss = RemoteSubscriptionService(config['jid'])
    ss.setHandlerParent(cs)
    ss.startService()

    # Set up web service that exposes the backend using REST

    root = resource.Resource()
    root.child_create = gateway.CreateResource(bs, config['jid'],
                                               config['jid'])
    root.child_delete = gateway.DeleteResource(bs, config['jid'],
                                               config['jid'])
    root.child_publish = gateway.PublishResource(bs, config['jid'],
                                                 config['jid'])
    root.child_subscribe = gateway.SubscribeResource(ss)
    root.child_unsubscribe = gateway.UnsubscribeResource(ss)
    root.child_list = gateway.ListResource(ss)

    site = server.Site(root)
    w = internet.TCPServer(int(config['webport']), channel.HTTPFactory(site))
    w.setServiceParent(s)

    # Set up a manhole

    namespace = {'service': s,
                 'component': cs,
                 'backend': bs}
    f = getManholeFactory(namespace, admin='admin')
    manholeService = strports.service('2222', f)
    manholeService.setServiceParent(s)

    return s
