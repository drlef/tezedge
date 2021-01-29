// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT
#![forbid(unsafe_code)]

use std::process::{Command, Output};

use tokio::time::{sleep, Duration};
use slog::{info, Logger};

pub const NODE_CONTAINER_NAME: &str = "deploy_rust-node_1";
pub const DEBUGGER_CONTAINER_NAME: &str = "deploy_rust-debugger_1";

// TODO: use external docker-compose for now, should we manage the images/containers directly?
pub async fn launch_stack(log: Logger) {
    start_with_compose(DEBUGGER_CONTAINER_NAME, "rust-debugger");
    // debugger healthcheck
    while reqwest::get("http://localhost:17732/v2/log").await.is_err() {
        sleep(Duration::from_millis(1000)).await;
    }
    info!(log, "Debugger for tezedge node is running");

    start_with_compose(NODE_CONTAINER_NAME, "rust-node");
    // node healthcheck
    while reqwest::get("http://localhost:18732/chains/main/blocks/head/header")
        .await
        .is_err()
    {
        sleep(Duration::from_millis(1000)).await;
    }
    info!(log, "Tezedge node is running");

    // start_with_compose("deploy_ocaml-node_1", "ocaml-node");
    // start_with_compose("deploy_ocaml-debugger_1", "ocaml-debugger");
    // info!(log, "Debugger for ocaml node started");
    // // node healthcheck
    // while reqwest::get("http://localhost:18733/chains/main/blocks/head/header")
    //     .await
    //     .is_err()
    // {
    //     sleep(Duration::from_millis(1000)).await;
    // }
    // info!(log, "Ocaml node is running");
}

pub async fn restart_stack(log: Logger) {
    stop_with_compose();
    cleanup_volumes();
    launch_stack(log).await;
}

pub async fn shutdown_and_update(log: Logger) {
    stop_with_compose();
    cleanup_docker();
    update_with_compose();
    restart_stack(log).await;
}

pub fn cleanup_docker() {
    cleanup_docker_system();
    cleanup_volumes();
}

pub fn start_with_compose(container_name: &str, service_ports_name: &str) -> Output {
    Command::new("docker-compose")
        .args(&[
            "-f",
            "deploy/docker-compose.debugger.yml",
            "run",
            "-d",
            "--name",
            container_name,
            "--service-ports",
            service_ports_name,
        ])
        .output()
        .expect("failed to execute docker-compose command")
}

pub fn stop_with_compose() -> Output {
    Command::new("docker-compose")
        .args(&["-f", "deploy/docker-compose.debugger.yml", "down"])
        .output()
        .expect("failed to execute docker-compose command")
}

pub fn update_with_compose() -> Output {
    Command::new("docker-compose")
        .args(&["-f", "deploy/docker-compose.debugger.yml", "pull"])
        .output()
        .expect("failed to execute docker-compose command")
}

pub fn cleanup_volumes() -> Output {
    Command::new("docker")
        .args(&["volume", "prune", "-f"])
        .output()
        .expect("failed to execute docker-compose command")
}

pub fn cleanup_docker_system() -> Output {
    Command::new("docker")
        .args(&["system", "prune", "-a", "-f"])
        .output()
        .expect("failed to execute docker-compose command")
}