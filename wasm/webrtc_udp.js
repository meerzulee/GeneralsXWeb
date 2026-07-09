// webrtc_udp.js — the JS half of the engine's UDP-over-WebRTC transport.
//
// The engine's UDP class (Core/GameEngine/Source/GameNetwork/udp.cpp) is shimmed
// under __EMSCRIPTEN__ to call into window.CafeUdp instead of BSD sockets. This
// file IS window.CafeUdp: it joins a cafe lobby room, opens one DataChannel per
// peer, and presents a datagram send/recv API that behaves like UDP.
//
// Addressing: every peer in the room gets a synthetic IP 10.0.0.N (N by sorted
// roster position, so all peers agree without server coordination). The engine
// binds ports and sends to (IP, port) exactly as over a real LAN; broadcast
// (255.255.255.255) fans out to every peer. The IP we report as the sender on
// receive is always the channel's mapped synthetic IP — the routable-identity
// invariant, so a reply to that IP reaches the same peer.
//
// Wire frame per DataChannel message (little-endian):
//   [0..1] srcPort   [2..3] destPort   [4..] payload
// srcIP is not in the frame; the receiver derives it from the channel's peer.
//
// Reliability: unreliable/unordered channel == UDP. The engine's own ACK/resend
// (Transport, ConnectionManager) handles reliability where it needs it.

(function () {
  "use strict";

  var BROADCAST = 0xffffffff; // INADDR_BROADCAST
  var ip2int = function (a, b, c, d) { return ((a << 24) | (b << 16) | (c << 8) | d) >>> 0; };
  var ipStr = function (n) { return [(n >>> 24) & 255, (n >>> 16) & 255, (n >>> 8) & 255, n & 255].join("."); };

  function CafeUdp() {
    this.id = null;
    this.host = false;
    this.iceServers = [];
    this.peers = new Map();   // peerId -> PeerLink
    this.roster = [];         // [{id,...}] latest
    this.myIP = 0;
    this.ipToPeer = new Map();// syntheticIP -> peerId
    this.inboxes = new Map(); // port -> [{ip, port, data:Uint8Array}]
    this.connected = false;
    this.log = function (m) { if (window.__udplog) window.__udplog("[udp] " + m); };
  }

  // ---- cafe signaling ----
  CafeUdp.prototype.connect = function (base, room, name) {
    var self = this;
    this.base = base;
    this.room = room;
    this.name = name;
    var wsUrl = base.replace(/^http/, "ws") +
      "/room/" + encodeURIComponent(room) + "/ws?name=" + encodeURIComponent(name || "engine");
    var ws = new WebSocket(wsUrl);
    this.ws = ws;
    ws.onmessage = function (e) { self._onMsg(JSON.parse(e.data)); };
    ws.onclose = function () { self.connected = false; self.log("signaling closed"); };
    ws.onerror = function () { self.log("signaling error"); };
    return new Promise(function (res) { self._welcome = res; });
  };

  CafeUdp.prototype._send = function (o) { this.ws.send(JSON.stringify(o)); };
  CafeUdp.prototype._signal = function (to, data) { this._send({ t: "signal", to: to, data: data }); };

  CafeUdp.prototype._onMsg = function (m) {
    switch (m.t) {
      case "welcome":
        this.id = m.youAre; this.host = m.host; this.iceServers = m.iceServers || [];
        this.connected = true;
        this.log("joined as " + this.id + (this.host ? " (host)" : ""));
        if (this._welcome) this._welcome(m);
        break;
      case "roster":
        this.roster = m.players;
        this._assignIPs(m.players);
        this._reconcile(m.players);
        break;
      case "signal":
        this._peer(m.from)._onSignal(m.data);
        break;
      case "start":
        this.lastStart = m; // {seed, map, slots} — used later to seed the engine
        this.log("START seed=" + m.seed);
        break;
    }
  };

  // Synthetic IP from the server-assigned stable slot: 10.0.0.<slot>. Stable for
  // the peer's whole session (unlike a client-side id sort that reshuffles as
  // peers join/leave), so the address a peer announces always routes back to it.
  CafeUdp.prototype._assignIPs = function (players) {
    this.ipToPeer.clear();
    var self = this;
    players.forEach(function (p) {
      var ip = ip2int(10, 0, 0, p.slot || 1);
      self.ipToPeer.set(ip, p.id);
      if (p.id === self.id) self.myIP = ip;
      var link = self.peers.get(p.id);
      if (link) link.syntheticIP = ip;
    });
    this.log("myIP=" + ipStr(this.myIP) + " peers=" + players.length);
  };

  CafeUdp.prototype._reconcile = function (players) {
    var others = new Set(players.map(function (p) { return p.id; })
      .filter(function (id) { return id !== this.id; }, this));
    for (var id of others) if (!this.peers.has(id)) this._peer(id);
    for (var pid of Array.from(this.peers.keys()))
      if (!others.has(pid)) { this.peers.get(pid).close(); this.peers.delete(pid); }
  };

  CafeUdp.prototype._peer = function (peerId) {
    var link = this.peers.get(peerId);
    if (!link) {
      link = new PeerLink(this, peerId);
      var e = this.roster.find(function (p) { return p.id === peerId; });
      this.peers.set(peerId, link);
      this._assignIPs(this.roster); // ensure the new link gets its IP
    }
    return link;
  };

  // ---- UDP API (called from EM_JS in the engine's UDP shim) ----
  CafeUdp.prototype.bind = function (port) {
    if (!this.inboxes.has(port)) this.inboxes.set(port, []);
    this.log("bind port " + port);
    return this.myIP >>> 0;
  };

  CafeUdp.prototype.send = function (destIP, destPort, srcPort, u8) {
    var frame = new Uint8Array(4 + u8.length);
    var dv = new DataView(frame.buffer);
    dv.setUint16(0, srcPort, true);
    dv.setUint16(2, destPort, true);
    frame.set(u8, 4);

    if (destIP === BROADCAST) {
      var n = 0;
      this.peers.forEach(function (link) { if (link.sendRaw(frame.buffer)) n++; });
      return n;
    }
    var pid = this.ipToPeer.get(destIP >>> 0);
    var link = pid && this.peers.get(pid);
    if (link && link.sendRaw(frame.buffer)) return 1;
    return 0; // peer unknown or channel not open — drop (UDP is lossy)
  };

  // recv returns {ip, port, data} or null (empty). ip/port are the SENDER's.
  CafeUdp.prototype.recv = function (port) {
    var inbox = this.inboxes.get(port);
    if (!inbox || inbox.length === 0) return null;
    return inbox.shift();
  };

  CafeUdp.prototype.close = function (port) { this.inboxes.delete(port); };
  CafeUdp.prototype.localIP = function () { return this.myIP >>> 0; };

  // Live connection status — the "is the host alive / connectable" helper. Read
  // from the HUD overlay (boot.html) or from JS: window.CafeUdp.status().
  CafeUdp.prototype.status = function () {
    var self = this;
    var host = this.roster.find(function (p) { return p.host; });
    var hostAlive = false;
    if (host) {
      if (host.id === this.id) hostAlive = true; // we are the host
      else {
        var link = this.peers.get(host.id);
        hostAlive = !!(link && link.channel && link.channel.readyState === "open");
      }
    }
    return {
      room: this.room || "?",
      connected: !!this.connected,
      myIP: ipStr(this.myIP),
      peers: this.roster.length,
      isHost: !!(host && host.id === this.id),
      hostAlive: hostAlive,
      hostIP: ipStr(this.hostIP()),
    };
  };

  // Switch to a different lobby room (the Direct Connect "room code" flow). Same
  // room = no-op (keeps the live connection, so hostIP() is immediate). A real
  // switch tears down the peers and reconnects; hostIP() is 0 until the new
  // roster arrives (~1s), so the caller may retry.
  CafeUdp.prototype.joinRoom = function (code) {
    code = String(code || "").trim();
    if (!code || code === this.room) return;
    this.log("switching room " + this.room + " -> " + code);
    this.peers.forEach(function (l) { l.close(); });
    this.peers.clear();
    this.ipToPeer.clear();
    this.roster = [];
    this.myIP = 0;
    try { this.ws && this.ws.close(); } catch (e) {}
    this.connect(this.base, code, this.name);
  };

  // Synthetic IP to direct-connect to: the room's game host (roster host flag),
  // or the sole other peer in a 1v1 room. 0 if not known yet.
  CafeUdp.prototype.hostIP = function () {
    var self = this;
    var pick = this.roster.find(function (p) { return p.host && p.id !== self.id; });
    if (!pick) {
      var others = this.roster.filter(function (p) { return p.id !== self.id; });
      if (others.length === 1) pick = others[0];
    }
    if (pick) {
      for (var ent of this.ipToPeer) if (ent[1] === pick.id) return ent[0] >>> 0;
    }
    return 0;
  };

  // Called by a PeerLink when a framed datagram arrives on its channel.
  CafeUdp.prototype._deliver = function (peerId, buf) {
    var link = this.peers.get(peerId);
    if (!link) return;
    var dv = new DataView(buf);
    var srcPort = dv.getUint16(0, true);
    var destPort = dv.getUint16(2, true);
    var inbox = this.inboxes.get(destPort);
    if (!inbox) return; // nothing bound to that port; drop
    inbox.push({ ip: link.syntheticIP >>> 0, port: srcPort, data: new Uint8Array(buf, 4) });
  };

  // ---- one WebRTC connection per peer (MDN perfect negotiation) ----
  function PeerLink(udp, peerId) {
    var self = this;
    this.udp = udp;
    this.peerId = peerId;
    this.syntheticIP = 0;
    this.polite = udp.id < peerId;
    var initiator = udp.id > peerId;
    this.makingOffer = false;
    this.ignoreOffer = false;

    var pc = new RTCPeerConnection({ iceServers: udp.iceServers });
    this.pc = pc;
    pc.onicecandidate = function (e) { if (e.candidate) udp._signal(peerId, { candidate: e.candidate }); };
    pc.onnegotiationneeded = function () {
      self.makingOffer = true;
      pc.setLocalDescription().then(function () {
        udp._signal(peerId, { description: pc.localDescription });
      }).catch(function (err) { udp.log("neg err " + err); })
        .finally(function () { self.makingOffer = false; });
    };
    pc.onconnectionstatechange = function () {
      if (pc.connectionState === "connected") udp.log("peer " + peerId + " ICE ok");
    };
    pc.ondatachannel = function (e) { self._bind(e.channel); };
    if (initiator)
      this._bind(pc.createDataChannel("game", { ordered: false, maxRetransmits: 0 }));
  }
  PeerLink.prototype._bind = function (ch) {
    var self = this;
    this.channel = ch;
    ch.binaryType = "arraybuffer";
    ch.onopen = function () { self.udp.log("channel OPEN to " + self.peerId); };
    ch.onmessage = function (e) { self.udp._deliver(self.peerId, e.data); };
  };
  PeerLink.prototype.sendRaw = function (buf) {
    if (this.channel && this.channel.readyState === "open") { this.channel.send(buf); return true; }
    return false;
  };
  PeerLink.prototype._onSignal = function (msg) {
    var pc = this.pc, self = this;
    var p;
    if (msg.description) {
      var collision = msg.description.type === "offer" &&
        (this.makingOffer || pc.signalingState !== "stable");
      this.ignoreOffer = !this.polite && collision;
      if (this.ignoreOffer) return;
      p = pc.setRemoteDescription(msg.description).then(function () {
        if (msg.description.type === "offer")
          return pc.setLocalDescription().then(function () {
            self.udp._signal(self.peerId, { description: pc.localDescription });
          });
      });
    } else if (msg.candidate) {
      p = pc.addIceCandidate(msg.candidate).catch(function (e) { if (!self.ignoreOffer) throw e; });
    }
    if (p) p.catch(function (e) { self.udp.log("signal err " + e); });
  };
  PeerLink.prototype.close = function () {
    try { this.channel && this.channel.close(); } catch (e) {}
    try { this.pc.close(); } catch (e) {}
  };

  // ---- boot ----
  // Config comes from boot.html: window.CAFE_URL, window.CAFE_ROOM, window.CAFE_NAME.
  var udp = new CafeUdp();
  window.CafeUdp = udp;
  window.CAFE_UDP_READY = udp.connect(
    window.CAFE_URL || "https://cafe-nw.mrz.sh",
    window.CAFE_ROOM || "lan",
    window.CAFE_NAME || ("engine-" + Math.floor(Math.random() * 1e4))
  ).then(function () { udp.log("ready"); });
})();
