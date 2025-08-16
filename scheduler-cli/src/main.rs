use anyhow::{bail, Context, Result};
use bytes::BytesMut;
use clap::{Parser, Subcommand};
use scheduler_core::{ClientRequest, Schedule, ServerResponse, TaskInfo, TaskSpec};
use std::{net::SocketAddr, path::PathBuf};
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use futures_util::SinkExt; // for framed.send()

#[derive(Parser, Debug)]
#[command(name = "scheduler-cli")]
struct Opts {
    /// 連線到 scheduler-server 的位址
    #[arg(long, default_value = "127.0.0.1:7878")]
    connect: String,

    /// 子命令
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// 新增任務
    Add {
        #[arg(long)]
        cmd: String,
        #[arg(long, num_args = 0.., value_delimiter = ' ')]
        args: Vec<String>,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value_t = true)]
        append: bool,
        #[arg(long)]
        once: Option<String>, // RFC3339
        #[arg(long)]
        daily: Option<String>, // "HH:MM"
        #[arg(long)]
        after: Option<u64>,
        #[arg(long, default_value_t = 0)]
        delay: u64,
    },

    /// 移除任務
    Remove {
        #[arg(long)]
        id: u64,
    },

    /// 列出所有任務
    List,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opts = Opts::parse();
    let addr: SocketAddr = opts.connect.parse().context("parse address")?;
    let stream = TcpStream::connect(addr).await?;
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());

    match opts.cmd {
        Cmd::Add {
            cmd,
            args,
            output,
            append,
            once,
            daily,
            after,
            delay,
        } => {
            let schedule = build_schedule(once, daily, after, delay)?;
            let spec = TaskSpec {
                cmd,
                args,
                output_path: output,
                append,
                schedule,
            };
            send_request(&mut framed, ClientRequest::AddTask(spec)).await?;
            if let Some(resp) = framed.next().await {
                let bytes: BytesMut = resp?;
                handle_response(&bytes).await?;
            }
        },

        Cmd::Remove { id } => {
            send_request(&mut framed, ClientRequest::RemoveTask { id }).await?;
            if let Some(resp) = framed.next().await {
                let bytes: BytesMut = resp?;
                handle_response(&bytes).await?;
            }
        },

        Cmd::List => {
            send_request(&mut framed, ClientRequest::ListTasks).await?;
            if let Some(resp) = framed.next().await {
                let bytes: BytesMut = resp?;
                handle_response(&bytes).await?;
            }
        },
    }

    Ok(())
}

async fn send_request(
    framed: &mut Framed<TcpStream, LengthDelimitedCodec>,
    req: ClientRequest,
) -> Result<()> {
    let bytes = serde_json::to_vec(&req)?;
    framed.send(bytes.into()).await?;
    Ok(())
}

async fn handle_response(bytes: &BytesMut) -> Result<()> {
    let resp: ServerResponse = serde_json::from_slice(&bytes[..])?;
    match resp {
        ServerResponse::Added { id } => {
            println!("✅ 任務已新增：id={}", id);
        }
        ServerResponse::Removed { ok } => {
            if ok {
                println!("🗑️ 任務已移除");
            } else {
                println!("⚠️ 找不到該任務 id，或移除失敗");
            }
        }
        ServerResponse::Tasks(list) => {
            if list.is_empty() {
                println!("（目前沒有任務）");
            } else {
                print_tasks(list);
            }
        }
        ServerResponse::Error(msg) => {
            bail!("❌ 伺服器錯誤：{msg}");
        }
    }
    Ok(())
}

fn print_tasks(list: Vec<TaskInfo>) {
    println!("=== 任務清單（共 {} 筆） ===", list.len());
    for t in list {
        println!("- id={} {:?}", t.id, t.spec);
        if let Some(rr) = t.last_result {
            println!(
                "  └─ 上次：status={}  at={}  stdout={}B  stderr={}B  -> {}",
                rr.status_code,
                rr.finished_at,
                rr.stdout_len,
                rr.stderr_len,
                rr.wrote_to.display()
            );
        }
    }
}

fn parse_daily_hhmm(s: &str) -> Result<(u32, u32)> {
    let parts: Vec<_> = s.split(':').collect();
    if parts.len() != 2 {
        bail!("每日時間請用 HH:MM，例如 08:00");
    }
    let hour: u32 = parts[0].parse().context("hour")?;
    let minute: u32 = parts[1].parse().context("minute")?;
    if hour > 23 || minute > 59 {
        bail!("時間超出範圍：{hour:02}:{minute:02}");
    }
    Ok((hour, minute))
}

fn build_schedule(
    once: Option<String>,
    daily: Option<String>,
    after: Option<u64>,
    delay: u64,
) -> Result<Schedule> {
    let mut cnt = 0;
    if once.is_some() { cnt += 1; }
    if daily.is_some() { cnt += 1; }
    if after.is_some() { cnt += 1; }

    if cnt == 0 {
        bail!("請至少指定一種排程：--once 或 --daily 或 --after");
    }
    if cnt > 1 {
        bail!("--once / --daily / --after 只能擇一使用");
    }

    if let Some(s) = once {
        let dt = chrono::DateTime::parse_from_rfc3339(&s)
            .with_context(|| format!("解析 RFC3339 失敗：{s}"))?;
        return Ok(Schedule::Once(dt));
    }
    if let Some(s) = daily {
        let (h, m) = parse_daily_hhmm(&s)?;
        return Ok(Schedule::Daily { hour: h, minute: m });
    }
    if let Some(id) = after {
        return Ok(Schedule::After { task_id: id, delay_secs: delay });
    }
    unreachable!()
}
