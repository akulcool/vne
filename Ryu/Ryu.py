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

class SPBSwitch(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(SPBSwitch, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.topology_data = nx.DiGraph()

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch()  # Match everything
        # Ensure that packet-in messages are sent to the controller for unknown flows.
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)

        # Add a low-priority flow to forward all unknown packets to the normal port
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_NORMAL)]
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=1, match=match, instructions=inst)
        datapath.send_msg(mod)

    def add_flow(self, datapath, priority, match, actions, buffer_id=ofproto_v1_3.OFP_NO_BUFFER, flags=0):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        mod = parser.OFPFlowMod(datapath=datapath, priority=priority, match=match,
                                instructions=inst, buffer_id=buffer_id,
                                out_port=ofproto.OFPP_ANY, out_group=ofproto.OFPG_ANY,
                                flags=flags)
        datapath.send_msg(mod)

    @set_ev_cls(event.EventLinkAdd)
    def link_add_handler(self, ev):
        src = ev.link.src
        dst = ev.link.dst
        self.topology_data.add_edge(src.dpid, dst.dpid, port=src.port_no)
        self.topology_data.add_edge(dst.dpid, src.dpid, port=dst.port_no)
        self.recompute_paths()

    def recompute_paths(self):
        # Recompute shortest paths in the network using Dijkstra's algorithm
        self.paths = dict(nx.all_pairs_dijkstra_path(self.topology_data))

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def _packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]
        dst = eth.dst
        src = eth.src

        self.logger.info("Packet in %s %s %s %s", dpid, src, dst, in_port)

        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})

        self.mac_to_port[dpid][src] = in_port

        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        data = None if msg.buffer_id == ofproto.OFP_NO_BUFFER else msg.data
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    def handle_ip(self, datapath, in_port, pkt, eth):
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        if ip_pkt:
            src_dpid = datapath.id
            dst_dpid = self.mac_to_port.get(eth.dst)
            path = self.paths.get(src_dpid, {}).get(dst_dpid, [])
            self.install_path_flows(datapath, path, in_port, eth)

    def install_path_flows(self, datapath, path, in_port, eth):
        # Install flow rules along the path for the IP packet
        for i in range(len(path) - 1):
            current_dpid = path[i]
            next_dpid = path[i + 1]
            current_switch = self.get_datapath(current_dpid)
            out_port = self.topology_data[current_dpid][next_dpid]['port']
            actions = [current_switch.ofproto_parser.OFPActionOutput(out_port)]
            match = current_switch.ofproto_parser.OFPMatch(in_port=in_port, eth_dst=eth.dst, eth_src=eth.src)
            self.add_flow(current_switch, 100, match, actions)

    def handle_arp(self, datapath, in_port, pkt, eth):
        arp_pkt = pkt.get_protocol(arp.arp)
        if arp_pkt:
            if arp_pkt.opcode == arp.ARP_REQUEST:
                self.process_arp_request(datapath, in_port, arp_pkt, eth)
            elif arp_pkt.opcode == arp.ARP_REPLY:
                self.process_arp_reply(datapath, in_port, arp_pkt, eth)

    def process_arp_request(self, datapath, in_port, arp_pkt, eth):
        # Check if the destination IP is known
        if arp_pkt.dst_ip in self.ip_to_mac:
            # Generate and send an ARP reply
            dst_mac = self.ip_to_mac[arp_pkt.dst_ip]
            arp_reply_pkt = self.create_arp_reply(arp_pkt, eth.src, dst_mac)
            self.send_packet(datapath, in_port, arp_reply_pkt)
        else:
            # Flood the ARP request to other ports
            self.flood(datapath, in_port, pkt.data)

    def process_arp_reply(self, datapath, in_port, arp_pkt, eth):
        # Update ARP table
        self.mac_to_port[eth.src] = in_port
        # No need to forward ARP replies typically, as they are usually unicast

    def create_arp_reply(self, arp_request, src_mac, dst_mac):
        # Create an ARP reply packet based on the ARP request
        arp_reply = arp.arp(opcode=arp.ARP_REPLY,
                            src_mac=dst_mac,
                            src_ip=arp_request.dst_ip,
                            dst_mac=src_mac,
                            dst_ip=arp_request.src_ip)
        return arp_reply

    def send_packet(self, datapath, port, pkt):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        actions = [parser.OFPActionOutput(port)]
        out = parser.OFPPacketOut(datapath=datapath, actions=actions, data=pkt.data)
        datapath.send_msg(out)

    def flood(self, datapath, port, data):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        # Flood packet out all ports except the port it came in on
        for p in datapath.ports:
            if p != port:
                actions = [parser.OFPActionOutput(p)]
                out = parser.OFPPacketOut(datapath=datapath, actions=actions, data=data)
                datapath.send_msg(out)