
from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
from twisted.internet import task
import json


def _address_from_peer_name(name):
    address, port = name.split(':', 1)
    return address, int(port)


class _Participant():
    """Base class for participants.""" 
    def make_connection(self, gossiper):
        """Attach this participant to a gossiper."""
        self.gossiper = gossiper


class PeerState(): 
    def __init__(self, participant, name=None):
        self.name = name
        self.participant = participant
        self.add_info = "VietStack"
        
    def set_name(self, name):
        self.name = name 


class Scuttle():
    def __init__(self, peers, local_peer):
        self.peers = peers
        self.local_peer = local_peer
    
    def digest(self):
        digest = {}
        for peer, state in self.peers.items():
            digest[peer] = state.add_info
        return digest
    
    def scuttle(self, digest):
        new_peers = []
        for peer, unused in digest.items():
            if not peer in self.peers:
                new_peers.append(peer)
        return new_peers


class Gossiper(DatagramProtocol):
    
    def __init__(self, clock, participant, address):
        self.participant = participant
        self._states = {}
        self.state = PeerState(participant)
        self._gossip_timer = task.LoopingCall(self._gossip)
        self._gossip_timer.clock = clock
        self._address = address
        self._scuttle = Scuttle(self._states, self.state)

    def _determine_endpoint(self):
        host = self.transport.getHost()
        if not self._address:
            self._address = host.host
            if self._address == '0.0.0.0':
                raise Exception("address not specified")
        return '%s:%d' % (self._address, host.port)

    def startProtocol(self):
        self.name = self._determine_endpoint()
        self.state.set_name(self.name)
        self._states[self.name] = self.state
        
        # Start gossip timer
        self._gossip_timer.start(10, now=True)
        self.participant.make_connection(self)

    def stopProtocol(self):
        """Stop gossip protocol"""
        self._gossip_timer.stop()
    
    def _new_peers_handling(self, peer_names):
        for name in peer_names:
            if name in self._states:
                continue
            self._states[name] = PeerState(self.participant, name=name)
            print "\nThe updated states: %s \n" %self._states
            print "*********" * 30
    
    def datagramReceived(self, data, address):
        print "The data received: %s" %json.loads(data)
        self._handle_message(json.loads(data), address)
    
    def _gossip(self):
        for peer in self._states.values():
            self._gossip_with_peer(peer)

    def _gossip_with_peer(self, peer):
        self.transport.write(json.dumps({
            'type': 'request', 'digest': self._scuttle.digest()
            }), _address_from_peer_name(peer.name))
        
    def _handle_message(self, message, address):
        """Handle an incoming message."""
        if message['type'] == 'request':
            self._handle_request(message, address)
        else:
            pass

    def _handle_request(self, message, address):
        
        new_peers = self._scuttle.scuttle(
            message['digest'])

        print "NEW_PEERS: %s" %new_peers
        self._new_peers_handling(new_peers)
        response = json.dumps({
            'type': 'first-response', 'message': 'From VietStack with love'
            })
        self.transport.write(response, address)


class Participant(_Participant):

    def __init__(self, name):
        self.name = name

members = []
limit = 20 
for i in range(0, limit):
    participant = Participant('127.0.0.1:%d' % (9000+i))
    gossiper = Gossiper(reactor, participant, '127.0.0.1')
    p = reactor.listenUDP(9000+i, gossiper)
    members.append((gossiper, p, participant))

for i in range(1, limit):
    print "\nTHE GOSSIP OBJECT INFORMATION:", members[i][0].__dict__
    members[i][0]._new_peers_handling(['127.0.0.1:9000'])

reactor.run()
