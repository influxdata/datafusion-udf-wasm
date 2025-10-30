//! WebAssembly linker code.

use std::sync::Arc;

use anyhow::{Context, Result};
use wasmtime::{
    Engine, Store,
    component::{Component, HasData, Linker},
};
use wasmtime_wasi::{ResourceTable, WasiView};

use crate::{
    WasmStateImpl,
    bindings::Datafusion,
    vfs::{HasFs, VfsView},
};

/// Link everything.
pub(crate) async fn link(
    engine: &Engine,
    component: &Component,
    state: WasmStateImpl,
) -> Result<(Arc<Datafusion>, Store<WasmStateImpl>)> {
    let mut store = Store::new(engine, state);

    let mut linker = Linker::new(engine);
    link_wasi_p2(&mut linker).context("link WASI p2")?;
    wasmtime_wasi_http::add_only_http_to_linker_async(&mut linker)
        .context("link WASI p2 HWasmStateImplWasmStateImplP")?;

    let bindings = Arc::new(
        Datafusion::instantiate_async(&mut store, component, &linker)
            .await
            .context("initialize bindings")?,
    );
    Ok((bindings, store))
}

/// Link WASIp2 interfaces.
fn link_wasi_p2(linker: &mut Linker<WasmStateImpl>) -> Result<()> {
    use wasmtime_wasi::{
        cli::{WasiCli, WasiCliView},
        clocks::{WasiClocks, WasiClocksView},
        p2::bindings,
        random::WasiRandom,
        sockets::{WasiSockets, WasiSocketsView},
    };

    let options = bindings::LinkOptions::default();
    wasmtime_wasi_io::bindings::wasi::io::error::add_to_linker::<WasmStateImpl, HasIo>(
        linker,
        |t| t.ctx().table,
    )?;
    wasmtime_wasi_io::bindings::wasi::io::poll::add_to_linker::<WasmStateImpl, HasIo>(
        linker,
        |t| t.ctx().table,
    )?;
    wasmtime_wasi_io::bindings::wasi::io::streams::add_to_linker::<WasmStateImpl, HasIo>(
        linker,
        |t| t.ctx().table,
    )?;
    bindings::clocks::wall_clock::add_to_linker::<WasmStateImpl, WasiClocks>(
        linker,
        WasmStateImpl::clocks,
    )?;
    bindings::clocks::monotonic_clock::add_to_linker::<WasmStateImpl, WasiClocks>(
        linker,
        WasmStateImpl::clocks,
    )?;
    bindings::cli::exit::add_to_linker::<WasmStateImpl, WasiCli>(
        linker,
        &(&options).into(),
        WasmStateImpl::cli,
    )?;
    bindings::cli::environment::add_to_linker::<WasmStateImpl, WasiCli>(
        linker,
        WasmStateImpl::cli,
    )?;
    bindings::cli::stdin::add_to_linker::<WasmStateImpl, WasiCli>(linker, WasmStateImpl::cli)?;
    bindings::cli::stdout::add_to_linker::<WasmStateImpl, WasiCli>(linker, WasmStateImpl::cli)?;
    bindings::cli::stderr::add_to_linker::<WasmStateImpl, WasiCli>(linker, WasmStateImpl::cli)?;
    bindings::cli::terminal_input::add_to_linker::<WasmStateImpl, WasiCli>(
        linker,
        WasmStateImpl::cli,
    )?;
    bindings::cli::terminal_output::add_to_linker::<WasmStateImpl, WasiCli>(
        linker,
        WasmStateImpl::cli,
    )?;
    bindings::cli::terminal_stdin::add_to_linker::<WasmStateImpl, WasiCli>(
        linker,
        WasmStateImpl::cli,
    )?;
    bindings::cli::terminal_stdout::add_to_linker::<WasmStateImpl, WasiCli>(
        linker,
        WasmStateImpl::cli,
    )?;
    bindings::cli::terminal_stderr::add_to_linker::<WasmStateImpl, WasiCli>(
        linker,
        WasmStateImpl::cli,
    )?;
    bindings::filesystem::types::add_to_linker::<WasmStateImpl, HasFs>(linker, WasmStateImpl::vfs)?;
    bindings::filesystem::preopens::add_to_linker::<WasmStateImpl, HasFs>(
        linker,
        WasmStateImpl::vfs,
    )?;
    bindings::random::random::add_to_linker::<WasmStateImpl, WasiRandom>(linker, |t| {
        t.ctx().ctx.random()
    })?;
    bindings::random::insecure::add_to_linker::<WasmStateImpl, WasiRandom>(linker, |t| {
        t.ctx().ctx.random()
    })?;
    bindings::random::insecure_seed::add_to_linker::<WasmStateImpl, WasiRandom>(linker, |t| {
        t.ctx().ctx.random()
    })?;
    bindings::sockets::instance_network::add_to_linker::<WasmStateImpl, WasiSockets>(
        linker,
        WasmStateImpl::sockets,
    )?;
    bindings::sockets::network::add_to_linker::<WasmStateImpl, WasiSockets>(
        linker,
        &(&options).into(),
        WasmStateImpl::sockets,
    )?;
    bindings::sockets::ip_name_lookup::add_to_linker::<WasmStateImpl, WasiSockets>(
        linker,
        WasmStateImpl::sockets,
    )?;
    bindings::sockets::tcp::add_to_linker::<WasmStateImpl, WasiSockets>(
        linker,
        WasmStateImpl::sockets,
    )?;
    bindings::sockets::tcp_create_socket::add_to_linker::<WasmStateImpl, WasiSockets>(
        linker,
        WasmStateImpl::sockets,
    )?;
    bindings::sockets::udp::add_to_linker::<WasmStateImpl, WasiSockets>(
        linker,
        WasmStateImpl::sockets,
    )?;
    bindings::sockets::udp_create_socket::add_to_linker::<WasmStateImpl, WasiSockets>(
        linker,
        WasmStateImpl::sockets,
    )?;
    Ok(())
}

/// Marker struct to tell linker that we do in fact provide IO-related resource tables.
struct HasIo;

impl HasData for HasIo {
    type Data<'a> = &'a mut ResourceTable;
}
