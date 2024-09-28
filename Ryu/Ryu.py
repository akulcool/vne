import networkx as nx
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet, arp, ipv4, ether_types
from ryu.lib import mac
from ryu.topology import event, switches
from ryu.topology.api import get_switch, get_link
from ryu.lib.packet import arp
import time

class SPBSwitch(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(SPBSwitch, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.topology_data = nx.DiGraph()
        self.idle_timeout = 300  # Set idle timeout for flow entries (5 minutes)
        self.resource_discovery_interval = 60  # Time interval for discovery (in seconds)
        self.last_resource_discovery = time.time()

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        # Initial setup of the switch
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch()  # Match everything
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)
        # Add a low-priority flow to forward all unknown packets to the normal port
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_NORMAL)]
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=1, match=match, instructions=inst)
        datapath.send_msg(mod)

    def add_flow(self, datapath, priority, match, actions, idle_timeout=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority, match=match,
                                instructions=inst, idle_timeout=idle_timeout or self.idle_timeout)
        datapath.send_msg(mod)

    @set_ev_cls(event.EventSwitchEnter)
    def resource_discovery(self, ev):
        # Called when a switch enters the network
        self.logger.info("Switch %s entered.", ev.switch.dp.id)
        # Add to topology
        self.update_topology()

    @set_ev_cls(event.EventLinkAdd)
    def link_discovered(self, ev):
        # Called when a link is discovered
        self.logger.info("Link discovered: %s -> %s", ev.link.src.dpid, ev.link.dst.dpid)
        # Update the topology when links are discovered
        self.update_topology()

    def update_topology(self):
        # Update the network topology graph using switches and links
        self.topology_data.clear()
        switches = get_switch(self, None)
        links = get_link(self, None)

        for switch in switches:
            self.topology_data.add_node(switch.dp.id)

        for link in links:
            self.topology_data.add_edge(link.src.dpid, link.dst.dpid)

    def mapping_decision(self, src_mac, dst_mac):
        # Use shortest path routing for mapping decisions
        if src_mac in self.topology_data and dst_mac in self.topology_data:
            path = nx.shortest_path(self.topology_data, src_mac, dst_mac)
            self.logger.info("Mapped path: %s", path)
            return path
        return None

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        # Handle incoming packets (ARP, etc.)
        msg = ev.msg
        datapath = msg.datapath
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)

        # Process ARP and other packet types here
        if eth.ethertype == ether_types.ETH_TYPE_ARP:
            arp_pkt = pkt.get_protocol(arp.arp)
            if arp_pkt:
                self.process_arp(datapath, in_port, pkt, eth, arp_pkt)

    def process_arp(self, datapath, in_port, pkt, eth, arp_pkt):
        # Handle ARP request and response
        if arp_pkt.opcode == arp.ARP_REQUEST:
            self.process_arp_request(datapath, in_port, arp_pkt, eth)
        elif arp_pkt.opcode == arp.ARP_REPLY:
            self.process_arp_reply(datapath, in_port, arp_pkt, eth)

    def node_deletion(self):
        # Periodically check and remove unused nodes
        current_time = time.time()
        if current_time - self.last_resource_discovery >= self.resource_discovery_interval:
            self.logger.info("Checking for unused nodes...")
            for node in list(self.topology_data.nodes):
                if node not in self.mac_to_port:  # Assuming mac_to_port represents active nodes
                    self.logger.info("Deleting unused node: %s", node)
                    self.topology_data.remove_node(node)
            self.last_resource_discovery = current_time

    def process_arp_request(self, datapath, in_port, arp_pkt, eth):
        # Check if destination IP is known
        if arp_pkt.dst_ip in self.mac_to_port:
            # Generate and send an ARP reply
            dst_mac = self.mac_to_port[arp_pkt.dst_ip]
            arp_reply_pkt = self.create_arp_reply(arp_pkt, eth.src, dst_mac)
            self.send_packet(datapath, in_port, arp_reply_pkt)
        else:
            # Flood ARP request to other ports
            self.flood(datapath, in_port, pkt.data)

    def process_arp_reply(self, datapath, in_port, arp_pkt, eth):
        # Update ARP table and install flow
        self.mac_to_port[eth.src] = in_port
        # No need to forward ARP replies typically, as they are unicast

    def flood(self, datapath, port, data):
        # Flood packet to all ports except incoming port
        for p in datapath.ports:
            if p != port:
                actions = [datapath.ofproto_parser.OFPActionOutput(p)]
                out = datapath.ofproto_parser.OFPPacketOut(datapath=datapath, actions=actions, data=data)
                datapath.send_msg(out)

    def send_packet(self, datapath, port, pkt):
        # Send a packet to a specific port
        actions = [datapath.ofproto_parser.OFPActionOutput(port)]
        out = datapath.ofproto_parser.OFPPacketOut(datapath=datapath, actions=actions, data=pkt.data)
        datapath.send_msg(out)
