//! JS の `connect(address)` を持つ値 (Workers VPC の binding) を Rust から呼ぶための extern。
//! workers-rs の `Fetcher` は `connect` を持たないので、JS の `connect()` を直接呼ぶ
//! (ippoan/alc-lineworks-worker の `src/tcp.rs` の写し、元は ippoan/alc-vein-worker。ippoan/workers-rs-containers-lab の lab-db にも同じ形がある)。

use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    pub(crate) type TcpPort;

    #[wasm_bindgen(method, catch)]
    pub(crate) fn connect(
        this: &TcpPort,
        address: &str,
    ) -> std::result::Result<worker::worker_sys::Socket, JsValue>;
}

/// Workers VPC の binding (`[[vpc_services]]`) を `env.get_binding::<TcpPort>()` で取り出せるようにする。
/// binding の JS 側の型名は公開されていないので、型名は見ずに値があるかだけで受ける
/// (無ければ `get_binding` が undefined として Err を返す)
impl worker::EnvBinding for TcpPort {
    const TYPE_NAME: &'static str = "Fetcher";

    fn get(val: JsValue) -> worker::Result<Self> {
        Ok(val.unchecked_into())
    }
}
