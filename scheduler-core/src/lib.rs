use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// 任務排程
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Schedule {
    /// 一次性（RFC3339 帶時區）
    Once(DateTime<FixedOffset>),
    /// 每日固定時間（本地時間）
    Daily { hour: u32, minute: u32 },
    /// 任務依賴：當 task_id 完成後觸發；可選延遲秒數
    After { task_id: u64, delay_secs: u64 },
}

/// 任務規格
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskSpec {
    pub cmd: String,
    pub args: Vec<String>,
    pub output_path: PathBuf,
    pub append: bool,
    pub schedule: Schedule,
}

/// 執行結果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    pub finished_at: DateTime<FixedOffset>,
    pub status_code: i32,
    pub stdout_len: usize,
    pub stderr_len: usize,
    pub wrote_to: PathBuf,
}

/// 任務資訊（給 list 用）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskInfo {
    pub id: u64,
    pub spec: TaskSpec,
    pub last_result: Option<RunResult>,
}

/// 客戶端 → 服務端
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientRequest {
    AddTask(TaskSpec),
    RemoveTask { id: u64 },
    ListTasks,
}

/// 服務端 → 客戶端
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerResponse {
    Added { id: u64 },
    Removed { ok: bool },
    Tasks(Vec<TaskInfo>),
    Error(String),
}

