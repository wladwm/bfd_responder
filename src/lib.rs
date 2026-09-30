extern crate mio;
#[macro_use]
extern crate log;
use std::net::SocketAddrV4;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub const DEFAULT_BFD_PORT: u16 = 3784;
pub const MIN_MAX_WAIT: std::time::Duration = std::time::Duration::from_micros(100);

#[repr(C, packed(1))]
#[derive(Copy, Clone)]
/// BFD version 1 packet
pub struct BfdPacket {
    pub version_and_code: u8,
    pub session_state_and_flags: u8,
    pub detect_multiplier: u8,
    pub message_length: u8,
    pub my_discriminator: u32,
    pub your_discriminator: u32,
    pub desired_min_tx_interval_ms: u32,
    pub required_min_rx_interval: u32,
    pub required_min_echo_interval: u32,
}
impl BfdPacket {
    pub fn new(
        session_state_and_flags: u8,
        detect_multiplier: u8,
        my_discriminator: u32,
        your_discriminator: u32,
        desired_min_tx_interval_ms: u32,
        required_min_rx_interval: u32,
        required_min_echo_interval: u32,
    ) -> Self {
        BfdPacket {
            version_and_code: 0x20,
            session_state_and_flags,
            detect_multiplier,
            message_length: 24,
            my_discriminator: my_discriminator.to_be(),
            your_discriminator: your_discriminator.to_be(),
            desired_min_tx_interval_ms: desired_min_tx_interval_ms.to_be(),
            required_min_rx_interval: required_min_rx_interval.to_be(),
            required_min_echo_interval: required_min_echo_interval.to_be(),
        }
    }
    pub fn my_discriminator(&self) -> u32 {
        u32::from_be(self.my_discriminator)
    }
    pub fn your_discriminator(&self) -> u32 {
        u32::from_be(self.your_discriminator)
    }
    pub fn desired_min_tx_interval(&self) -> u32 {
        u32::from_be(self.desired_min_tx_interval_ms)
    }
    pub fn required_min_rx_interval(&self) -> u32 {
        u32::from_be(self.required_min_rx_interval)
    }
    pub fn required_min_echo_interval(&self) -> u32 {
        u32::from_be(self.required_min_echo_interval)
    }
}
impl std::convert::AsRef<[u8]> for BfdPacket {
    fn as_ref(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const BfdPacket as *const u8,
                std::mem::size_of::<Self>(),
            )
        }
    }
}
#[derive(Debug, PartialEq, Eq)]
pub enum BfdState {
    AdminDown,
    Down,
    Init,
    Up,
}
#[derive(Debug)]
pub struct BfdSession {
    pub peer: SocketAddrV4,
    pub state: u8,
    pub last_sent: Instant,
    pub last_recv: Instant,
    pub my_discriminator: u32,
    pub your_discriminator: u32,
    pub detect_multiplier: u8,
    pub tx: u32,
    pub rx: u32,
    pub min_echo: u32,
}
impl BfdSession {
    pub fn new(from: SocketAddrV4, bfd: &BfdPacket) -> BfdSession {
        let n = Instant::now();
        BfdSession {
            peer: from,
            state: bfd.session_state_and_flags,
            last_sent: n,
            last_recv: n,
            my_discriminator: if bfd.your_discriminator() == 0 {
                bfd.my_discriminator() + 1
            } else {
                bfd.your_discriminator
            },
            your_discriminator: bfd.my_discriminator(),
            detect_multiplier: bfd.detect_multiplier,
            rx: bfd.desired_min_tx_interval(),
            tx: bfd.required_min_rx_interval(),
            min_echo: bfd.required_min_echo_interval(),
        }
    }
    fn tick(&self) -> BfdPacket {
        BfdPacket::new(
            self.state,
            self.detect_multiplier,
            self.my_discriminator,
            self.your_discriminator,
            self.tx,
            self.rx,
            self.min_echo,
        )
    }
    pub fn duration_tx(&self) -> std::time::Duration {
        Duration::from_micros(self.tx.into())
    }
    pub fn state(&self) -> BfdState {
        match (self.state >> 6) & 3 {
            0 => BfdState::AdminDown,
            1 => BfdState::Down,
            2 => BfdState::Init,
            3 => BfdState::Up,
            _ => BfdState::AdminDown,
        }
    }
    pub fn is_down(&self) -> bool {
        ((self.state >> 6) & 3) < 2
    }
    pub fn is_up(&self) -> bool {
        ((self.state >> 6) & 3) > 1
    }
    pub fn set_state(&mut self, state: BfdState) {
        match state {
            BfdState::AdminDown => self.state &= 0x3f,
            BfdState::Down => self.state = (self.state & 0x3f) | 0x40,
            BfdState::Init => self.state = (self.state & 0x3f) | 0x80,
            BfdState::Up => self.state = (self.state & 0x3f) | 0xc0,
        }
    }
}
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct BfdKey {
    peer: SocketAddrV4,
    my_discriminator: u32,
}
impl BfdKey {
    fn new(session: &BfdSession) -> BfdKey {
        BfdKey {
            peer: session.peer,
            my_discriminator: session.my_discriminator,
        }
    }
}
/// BfdResponder builder
#[derive(Default)]
pub struct BfdResponderOptions {
    listen_point: Option<SocketAddrV4>,
    socket: Option<std::net::UdpSocket>,
    target_port: u16,
}
impl BfdResponderOptions {
    /// with default options
    pub fn new() -> BfdResponderOptions {
        BfdResponderOptions {
            listen_point: None,
            socket: None,
            target_port: DEFAULT_BFD_PORT,
        }
    }
    /// with specified bind IP address
    pub fn with_bind_address(mut self, ip: std::net::Ipv4Addr) -> BfdResponderOptions {
        self.listen_point = Some(SocketAddrV4::new(ip, DEFAULT_BFD_PORT));
        self
    }
    /// with specified bind IP address and port
    pub fn with_bind_address_port(mut self, sa: SocketAddrV4) -> BfdResponderOptions {
        self.listen_point = Some(sa);
        self
    }
    /// with specified peer UDP port
    pub fn with_target_port(mut self, port: u16) -> BfdResponderOptions {
        self.target_port = port;
        self
    }
    /// with prepared bound non-blocking standard socket
    pub fn from_socket(mut self, socket: std::net::UdpSocket) -> BfdResponderOptions {
        self.socket = Some(socket);
        self
    }
    /// runs responder
    pub fn run(self) -> std::io::Result<BfdResponder> {
        let r = BfdResponder::new();
        r.run(self)?;
        Ok(r)
    }
}
/// BfdResponder
pub struct BfdResponder {
    stop: Arc<std::sync::atomic::AtomicBool>,
    pub join: std::sync::Mutex<Option<JoinHandle<std::io::Result<()>>>>,
}
impl BfdResponder {
    pub(crate) fn new() -> BfdResponder {
        BfdResponder {
            stop: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            join: std::sync::Mutex::new(None),
        }
    }
    fn run(&self, mut opts: BfdResponderOptions) -> std::io::Result<()> {
        self.stop.store(false, std::sync::atomic::Ordering::SeqCst);
        let socket = match opts.socket.take() {
            Some(s) => {
                s.set_nonblocking(true)?;
                mio::net::UdpSocket::from_std(s)
            }
            None => {
                let addr = opts
                    .listen_point
                    .take()
                    .unwrap_or(std::net::SocketAddrV4::new(
                        std::net::Ipv4Addr::UNSPECIFIED,
                        DEFAULT_BFD_PORT,
                    ));
                mio::net::UdpSocket::bind(std::net::SocketAddr::V4(addr))?
            }
        };
        let ss = self.stop.clone();
        let target_port = opts.target_port;
        let jh = std::thread::spawn(move || Self::run_thread(socket, target_port, ss));
        let mut grd = self.join.lock().unwrap();
        *grd = Some(jh);
        Ok(())
    }
    fn run_thread(
        mut socket: mio::net::UdpSocket,
        target_port: u16,
        stop: Arc<std::sync::atomic::AtomicBool>,
    ) -> std::io::Result<()> {
        let mut buf = [0u8; 1500];
        let mut poll_read = mio::Poll::new()?;
        let mut events = mio::Events::with_capacity(1);
        poll_read
            .registry()
            .register(&mut socket, mio::Token(0), mio::Interest::READABLE)?;
        let bfd = unsafe { &mut *(buf.as_mut_ptr() as *mut BfdPacket) };
        let mut sessions = std::collections::BTreeMap::<BfdKey, BfdSession>::new();
        while !stop.load(std::sync::atomic::Ordering::Relaxed) {
            // send ticks
            let mut now = Instant::now();
            for (_, sess) in sessions.iter_mut() {
                if sess.is_up() && (sess.last_sent + sess.duration_tx()) >= now {
                    if sess.last_recv
                        + Duration::from_micros((sess.rx * (sess.detect_multiplier as u32)).into())
                        < now
                    {
                        sess.set_state(BfdState::Down); // mark session as down and send last packet
                        warn!("BFD session {:?} timed out", sess);
                    }
                    debug!("Send BFD {:?}", sess);
                    let pck = sess.tick();
                    loop {
                        let sz = match socket
                            .send_to(pck.as_ref(), std::net::SocketAddr::V4(sess.peer))
                        {
                            Ok(s) => s,
                            Err(e) => {
                                if e.kind() != std::io::ErrorKind::WouldBlock {
                                    return Err(e.into());
                                }
                                continue;
                            }
                        };
                        if sz != std::mem::size_of::<BfdPacket>() {
                            return Err(std::io::ErrorKind::Interrupted.into());
                        }
                        break;
                    }
                    debug!("Sent BFD {:?}", sess);
                    if sess.state() == BfdState::Init {
                        sess.set_state(BfdState::Up);
                    }
                    now = Instant::now();
                    sess.last_sent = now;
                }
            }
            let mut max_wait = sessions
                .iter()
                .filter(|(_, sess)| sess.is_up())
                .map(|(_, sess)| (sess.last_sent + sess.duration_tx()) - now)
                .fold(
                    Duration::from_millis(1000),
                    |acc, x| if acc < x { acc } else { x },
                );
            if max_wait < MIN_MAX_WAIT {
                max_wait = MIN_MAX_WAIT;
            }
            debug!("BFD max wait {:?}", max_wait);

            // receive tick
            let res = match socket.recv_from(&mut buf) {
                Ok(r) => r,
                Err(e) => {
                    if e.kind() != std::io::ErrorKind::WouldBlock {
                        return Err(e.into());
                    }
                    if let Err(err) = poll_read.poll(&mut events, Some(max_wait)) {
                        if err.kind() == std::io::ErrorKind::TimedOut {
                            continue;
                        }
                        return Err(err);
                    }
                    continue;
                }
            };
            if res.0 != std::mem::size_of::<BfdPacket>() {
                // unknown garbage packet received
                trace!("Incorrect BFD packet received from {}", res.1);
                continue;
            }
            if let std::net::SocketAddr::V4(s4) = &res.1 {
                let mut gsess = BfdSession::new(*s4, bfd);
                gsess.peer.set_port(target_port);
                let skey = BfdKey::new(&gsess);
                if let Some(sess) = sessions.get_mut(&skey) {
                    debug!("Old BFD session {:?} for {:?}", sess, gsess);
                    sess.last_recv = gsess.last_recv;
                    //sess.state = bfd.session_state_and_flags;
                    if sess.is_down() {
                        sess.state = 0;
                        sess.set_state(BfdState::Init);
                    }
                } else {
                    debug!("New BFD session {:?}", gsess);
                    gsess.state = 0;
                    gsess.set_state(BfdState::Init);
                    sessions.insert(skey, gsess);
                }
            }
        }
        Ok(())
    }
    /// simply stops running responder
    pub fn stop(&self) -> std::io::Result<()> {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(jh) = self.join.lock().unwrap().take() {
            match jh.join() {
                Err(_) => Ok(()),
                Ok(r) => r,
            }
        } else {
            Ok(())
        }
    }
}
