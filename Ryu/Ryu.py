import networkx as nx
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet, arp, ipv4, ether_types
from ryu.topology import event
from ryu.topology.api import get_switch, get_link
from ryu.lib.packet import arp

class SPBSwitch(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *args, **kwargs):
        super(SPBSwitch, self).__init__(*args, **kwargs)
        self.mac_to_port = {}
        self.ip_to_mac = {}  # Track IP-to-MAC mappings
        self.topology_data = nx.DiGraph()
        self.datapaths = {}  # Store datapaths for later access

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        match = parser.OFPMatch()  # Match everything

        # Send unknown packets to the controller
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER, ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)
        self.datapaths[datapath.id] = datapath  # Store datapath for future use

    def add_flow(self, datapath, priority, match, actions, buffer_id=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS, actions)]
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id, priority=priority,
                                    match=match, instructions=inst)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority, match=match, instructions=inst)
        datapath.send_msg(mod)

    @set_ev_cls(event.EventLinkAdd)
    def link_add_handler(self, ev):
        src = ev.link.src
        dst = ev.link.dst
        self.topology_data.add_edge(src.dpid, dst.dpid, port=src.port_no)
        self.topology_data.add_edge(dst.dpid, src.dpid, port=dst.port_no)
        self.recompute_paths()

    def recompute_paths(self):
        # Compute all shortest paths in the topology
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
        eth_type = eth.ethertype
        src = eth.src
        dst = eth.dst
        dpid = datapath.id

        self.mac_to_port.setdefault(dpid, {})
        self.mac_to_port[dpid][src] = in_port  # Learn MAC address

        # If the packet is ARP, handle ARP request/reply
        if eth_type == ether_types.ETH_TYPE_ARP:
            self.handle_arp(datapath, in_port, pkt, eth)
            return

        # If the packet is IPv4, forward based on path computation
        if eth_type == ether_types.ETH_TYPE_IP:
            self.handle_ip(datapath, in_port, pkt, eth)
            return

        # If the destination MAC is already known, forward it
        if dst in self.mac_to_port[dpid]:
            out_port = self.mac_to_port[dpid][dst]
        else:
            out_port = ofproto.OFPP_FLOOD  # Flood if destination is unknown

        actions = [parser.OFPActionOutput(out_port)]

        # Install a flow to avoid future packet_in events
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            self.add_flow(datapath, 1, match, actions)

        # Send the packet out
        data = msg.data if msg.buffer_id == ofproto.OFP_NO_BUFFER else None
        out = parser.OFPPacketOut(datapath=datapath, buffer_id=msg.buffer_id,
                                  in_port=in_port, actions=actions, data=data)
        datapath.send_msg(out)

    def handle_ip(self, datapath, in_port, pkt, eth):
        ip_pkt = pkt.get_protocol(ipv4.ipv4)
        if ip_pkt:
            src_dpid = datapath.id
            dst_dpid = self.get_host_location(eth.dst)
            if dst_dpid is None:
                return

            # Find the path between source and destination
            path = self.paths.get(src_dpid, {}).get(dst_dpid, None)
            if path:
                self.install_path_flows(path, in_port, eth)

    def install_path_flows(self, path, in_port, eth):
        # Install flows along the path
        for i in range(len(path) - 1):
            current_dpid = path[i]
            next_dpid = path[i + 1]
            current_switch = self.datapaths[current_dpid]
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
        # Check if we know the MAC for the destination IP
        if arp_pkt.dst_ip in self.ip_to_mac:
            dst_mac = self.ip_to_mac[arp_pkt.dst_ip]
            arp_reply = self.create_arp_reply(arp_pkt, eth.src, dst_mac)
            self.send_packet(datapath, in_port, arp_reply)
        else:
            self.flood(datapath, in_port, arp_pkt)

    def process_arp_reply(self, datapath, in_port, arp_pkt, eth):
        # Learn the IP-MAC mapping
        self.ip_to_mac[arp_pkt.src_ip] = eth.src

    def create_arp_reply(self, arp_request, src_mac, dst_mac):
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

    def flood(self, datapath, in_port, data):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        for p in datapath.ports.keys():
            if p != in_port:
                actions = [parser.OFPActionOutput(p)]
                out = parser.OFPPacketOut(datapath=datapath, actions=actions, data=data)
                datapath.send_msg(out)

    def get_host_location(self, mac):
        # Look up the switch DPID where the MAC is located
        for dpid, mac_table in self.mac_to_port.items():
            if mac in mac_table:
                return dpid
        return None
