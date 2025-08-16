use anyhow::{Context, Result};
use bytes::BytesMut;
use chrono::{DateTime, FixedOffset, Local, Timelike};
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use scheduler_core::{ClientRequest, RunResult, Schedule, ServerResponse, TaskInfo, TaskSpec};
use std::{
    collections::VecDeque,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    process::Command,
    time::sleep,
};
use tokio_util::{
    codec::{Framed, LengthDelimitedCodec},
    sync::CancellationToken,
};

/// 每個任務的狀態
#[derive(Debug)]
struct TaskEntry {
    spec: TaskSpec,
    cancel: Option<CancellationToken>,          // 只給 Once/Daily 用；After 不需要
    last_result: Arc<Mutex<Option<RunResult>>>, // 同步鎖，避免非 Send await
}

/// 伺服器全域狀態
struct State {
    tasks: DashMap<u64, TaskEntry>,       // 任務表
    watchers: DashMap<u64, Vec<u64>>,     // 依賴：A -> [B..]（A 完成後觸發 B）
    next_id: AtomicU64,                   // 遞增任務 ID
    data_path: PathBuf,                   // 持久化檔案
}

#[tokio::main]
async fn main() -> Result<()> {
    // 可改 clap 參數；先用固定值方便跑起來
    let bind = "127.0.0.1:7878".to_string();
    let data = PathBuf::from("tasks.json");

    let state = Arc::new(State {
        tasks: DashMap::new(),
        watchers: DashMap::new(),
        next_id: AtomicU64::new(1),
        data_path: data.clone(),
    });

    // 啟動時載入持久化任務
    if data.exists() {
        if let Err(e) = load_persisted(&state, &data).await {
            eprintln!("load persisted error: {e:?}");
        }
    }

    let listener = TcpListener::bind(&bind).await?;
    println!("✅ scheduler-server listening on {bind}");

    loop {
        let (stream, peer) = listener.accept().await?;
        let st = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(st, stream, peer).await {
                eprintln!("connection {peer} error: {e:?}");
            }
        });
    }
}

/// 單一連線：收 ClientRequest → 回 ServerResponse
async fn handle_conn(state: Arc<State>, stream: TcpStream, _peer: SocketAddr) -> Result<()> {
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());

    while let Some(frame) = framed.next().await {
        let bytes: BytesMut = frame?;
        let req: ClientRequest = serde_json::from_slice(&bytes[..])?;

        let resp = match req {
            ClientRequest::AddTask(spec) => {
                let id = add_task(&state, spec).await?;
                ServerResponse::Added { id }
            }
            ClientRequest::RemoveTask { id } => {
                let ok = remove_task(&state, id).await?;
                ServerResponse::Removed { ok }
            }
            ClientRequest::ListTasks => {
                let mut list = Vec::new();
                for kv in state.tasks.iter() {
                    let id = *kv.key();
                    let ent = kv.value();
                    let last = ent.last_result.lock().unwrap().clone(); // 同步鎖，無 await
                    list.push(TaskInfo {
                        id,
                        spec: ent.spec.clone(),
                        last_result: last,
                    });
                }
                ServerResponse::Tasks(list)
            }
        };

        let out = serde_json::to_vec(&resp)?;
        framed.send(out.into()).await?;
    }

    Ok(())
}

/// 新增任務：為 Once/Daily 啟動排程；After 只登記依賴
async fn add_task(state: &Arc<State>, spec: TaskSpec) -> Result<u64> {
    let id = state.next_id.fetch_add(1, Ordering::SeqCst);

    let base = TaskEntry {
        spec: spec.clone(),
        cancel: None,
        last_result: Arc::new(Mutex::new(None)),
    };

    let entry = match &spec.schedule {
        Schedule::After { task_id, .. } => {
            state.watchers.entry(*task_id).or_default().push(id);
            base
        }
        Schedule::Once(_) | Schedule::Daily { .. } => {
            let tok = CancellationToken::new();
            spawn_scheduler_loop(id, spec.clone(), tok.clone(), state.clone());
            TaskEntry {
                cancel: Some(tok),
                ..base
            }
        }
    };

    state.tasks.insert(id, entry);
    persist(state).await?;
    Ok(id)
}

/// 移除任務：取消（若有）並維護依賴
async fn remove_task(state: &Arc<State>, id: u64) -> Result<bool> {
    if let Some((_, mut ent)) = state.tasks.remove(&id) {
        if let Some(tok) = ent.cancel.take() {
            tok.cancel();
        }
        for mut kv in state.watchers.iter_mut() {
            kv.value_mut().retain(|&x| x != id);
        }
        persist(state).await?;
        return Ok(true);
    }
    Ok(false)
}

/// 為 Once/Daily 啟動一個 scheduler 迴圈（依賴任務不走這裡）
fn spawn_scheduler_loop(
    id: u64,
    spec: TaskSpec,
    cancel: CancellationToken,
    state: Arc<State>,
) {
    tokio::spawn(async move {
        loop {
            let next_time: DateTime<FixedOffset> = match &spec.schedule {
                Schedule::Once(t) => *t, // 已是 FixedOffset
                Schedule::Daily { hour, minute } => next_daily_at(*hour, *minute),
                Schedule::After { .. } => unreachable!("After doesn't use loop"),
            };

            let wait = duration_to(next_time);
            println!(
                "⏰ task {} scheduled at {} ({}s later)",
                id,
                next_time,
                wait.as_secs()
            );

            tokio::select! {
                _ = sleep(wait) => {
                    if let Err(e) = run_once_and_record(id, spec.clone(), state.clone()).await {
                        eprintln!("task {} run error: {e:?}", id);
                    }
                    if matches!(spec.schedule, Schedule::Once(_)) { break; }
                }
                _ = cancel.cancelled() => {
                    println!("task {} cancelled", id);
                    break;
                }
            }
        }
    });
}

/// 只負責「執行一次 + 記錄結果」（不處理依賴、不遞迴）
async fn execute_once(id: u64, spec: &TaskSpec, state: &Arc<State>) -> Result<()> {
    // 1) 執行外部程式
    let output = Command::new(&spec.cmd)
        .args(&spec.args)
        .output()
        .await
        .with_context(|| format!("spawn {:?}", spec.cmd))?;
    let status = output.status.code().unwrap_or(-1);
    let now = local_now_fixed(); // FixedOffset

    // 2) 寫檔（同步 I/O，無 await）
    {
        ensure_parent_dir(&spec.output_path)?;
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(spec.append)
            .open(&spec.output_path)?;
        use std::io::Write;
        writeln!(f, "=== [{}] task {} exit {} ===", now, id, status)?;
        if !output.stdout.is_empty() {
            f.write_all(&output.stdout)?;
            if !spec.append { writeln!(f)?; }
        }
        if !output.stderr.is_empty() {
            writeln!(f, "\n--- stderr ---")?;
            f.write_all(&output.stderr)?;
            writeln!(f)?;
        }
    }

    // 3) 更新 last_result（同步鎖）
    if let Some(ent) = state.tasks.get(&id) {
        let last = ent.value().last_result.clone();
        drop(ent);
        let mut g = last.lock().unwrap();
        *g = Some(RunResult {
            finished_at: now,
            status_code: status,
            stdout_len: output.stdout.len(),
            stderr_len: output.stderr.len(),
            wrote_to: spec.output_path.clone(),
        });
    }

    Ok(())
}

/// 執行當前任務，並「迭代」展開整條依賴鏈（不遞迴、不 spawn）
async fn run_once_and_record(id: u64, spec: TaskSpec, state: Arc<State>) -> Result<()> {
    // 先跑當前任務
    execute_once(id, &spec, &state).await?;

    // 準備 queue：待執行的依賴 (dep_id, spec, delay_secs)
    let mut q: VecDeque<(u64, TaskSpec, u64)> = VecDeque::new();

    // 第一層依賴（複製資料，避免持有 guard 跨 await）
    if let Some(dependents) = state.watchers.get(&id) {
        for dep_id in dependents.value().clone() {
            if let Some(ent) = state.tasks.get(&dep_id) {
                if let Schedule::After { task_id, delay_secs } = ent.value().spec.schedule.clone() {
                    if task_id == id {
                        q.push_back((dep_id, ent.value().spec.clone(), delay_secs));
                    }
                }
            }
        }
    }

    // 逐一處理 queue（BFS/迭代）
    while let Some((cur_id, cur_spec, delay_secs)) = q.pop_front() {
        if delay_secs > 0 {
            sleep(Duration::from_secs(delay_secs)).await;
        }
        if let Err(e) = execute_once(cur_id, &cur_spec, &state).await {
            eprintln!("dependent task {} run error: {:?}", cur_id, e);
            // 不中斷鏈，繼續處理後續依賴
        }

        // 推展「以 cur_id 為前置」的後續依賴
        if let Some(dependents) = state.watchers.get(&cur_id) {
            for dep_id in dependents.value().clone() {
                if let Some(ent) = state.tasks.get(&dep_id) {
                    if let Schedule::After { task_id, delay_secs } =
                        ent.value().spec.schedule.clone()
                    {
                        if task_id == cur_id {
                            q.push_back((dep_id, ent.value().spec.clone(), delay_secs));
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

// ===== 持久化：最小實作 =====
async fn persist(state: &Arc<State>) -> Result<()> {
    #[derive(serde::Serialize)]
    struct Rec {
        id: u64,
        spec: TaskSpec,
    }

    let mut arr = Vec::new();
    for kv in state.tasks.iter() {
        arr.push(Rec {
            id: *kv.key(),
            spec: kv.value().spec.clone(),
        });
    }

    let s = serde_json::to_string_pretty(&arr)?;
    std::fs::write(&state.data_path, s)?;
    Ok(())
}

async fn load_persisted(state: &Arc<State>, path: &Path) -> Result<()> {
    #[derive(serde::Deserialize)]
    struct Rec {
        id: u64,
        spec: TaskSpec,
    }

    let bytes = std::fs::read(path)?;
    let list: Vec<Rec> = serde_json::from_slice(&bytes[..])?;
    let mut max_id = 0u64;

    for r in list {
        max_id = max_id.max(r.id);

        let base = TaskEntry {
            spec: r.spec.clone(),
            cancel: None,
            last_result: Arc::new(Mutex::new(None)),
        };

        let entry = match &r.spec.schedule {
            Schedule::After { task_id, .. } => {
                state.watchers.entry(*task_id).or_default().push(r.id);
                base
            }
            Schedule::Once(_) | Schedule::Daily { .. } => {
                let tok = CancellationToken::new();
                spawn_scheduler_loop(r.id, r.spec.clone(), tok.clone(), state.clone());
                TaskEntry {
                    cancel: Some(tok),
                    ..base
                }
            }
        };

        state.tasks.insert(r.id, entry);
    }

    state
        .next_id
        .store(max_id.saturating_add(1), Ordering::SeqCst);
    Ok(())
}

// ===== 時間/工具（統一 FixedOffset） =====
fn ensure_parent_dir(p: &Path) -> Result<()> {
    if let Some(parent) = p.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

fn next_daily_at(hour: u32, minute: u32) -> DateTime<FixedOffset> {
    let now = Local::now();
    let today = now
        .with_hour(hour)
        .and_then(|t| t.with_minute(minute))
        .and_then(|t| t.with_second(0))
        .unwrap();
    let next_local = if today > now {
        today
    } else {
        today + chrono::Duration::days(1)
    };
    next_local.fixed_offset()
}

fn duration_to(when: DateTime<FixedOffset>) -> Duration {
    let now = Local::now().fixed_offset();
    let secs = (when - now).num_seconds().max(0) as u64;
    Duration::from_secs(secs)
}

fn local_now_fixed() -> DateTime<FixedOffset> {
    Local::now().fixed_offset()
}
