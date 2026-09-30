extern crate bfd_responder;

fn run_responder() -> std::io::Result<()> {
    println!("Try to listen any incoming BFD packets on 0.0.0.0:3784");
    let responder = bfd_responder::BfdResponderOptions::new().run()?;
    let grd = responder.join.lock().unwrap().take();
    if let Some(jh) = grd {
        match jh.join() {
            Err(_) => Ok(()),
            Ok(r) => r,
        }
    } else {
        Ok(())
    }
}

fn main() {
    if let Err(e) = run_responder() {
        eprintln!("Run error: {:?}", e);
    }
}
