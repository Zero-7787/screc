use chrono::{DateTime, Local};
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use screc_core::config::{AppConfig, ModelEntry};

/// GUI → 录制管理器的命令
pub enum ModelCommand {
    Enable(String),
    Disable(String),
    Add(String, bool),
    Remove(String),
    /// 启动所有已启用模特的录制
    StartAll,
    /// 停止所有录制
    StopAll,
}

/// 模特状态信息（用于 GUI 展示）
#[derive(Debug, Clone)]
pub struct ModelStatus {
    pub username: String,
    pub enabled: bool,
    pub status: ModelStreamStatus,
    pub is_recording: bool,
    pub recording_start: Option<DateTime<Local>>,
    pub last_check: Option<DateTime<Local>>,
    pub file_path: Option<String>,
}

/// 模特直播状态（映射自 StreamStatus）
#[derive(Debug, Clone, PartialEq)]
pub enum ModelStreamStatus {
    Public,
    Private,
    Offline,
    LongOffline,
    Error,
    Unknown,
    NotExist,
    Restricted,
    Blocked,
}

impl ModelStreamStatus {
    pub fn label(&self) -> &str {
        match self {
            Self::Public => "在线",
            Self::Private => "私人秀",
            Self::Offline => "离线",
            Self::LongOffline => "长时间离线",
            Self::Error => "错误",
            Self::Unknown => "未知",
            Self::NotExist => "不存在",
            Self::Restricted => "受限",
            Self::Blocked => "已封禁",
        }
    }
}

/// 日志条目
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub timestamp: DateTime<Local>,
    pub level: LogLevel,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogLevel {
    Info,
    Warn,
    Error,
    Debug,
}

impl LogLevel {
    pub fn label(&self) -> &str {
        match self {
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Error => "ERROR",
            Self::Debug => "DEBUG",
        }
    }
}

/// 全局共享状态
#[derive(Clone)]
pub struct SharedGuiState {
    inner: Arc<Mutex<SharedGuiStateInner>>,
}

struct SharedGuiStateInner {
    models: Vec<ModelStatus>,
    /// 模特数据变化时递增，供 GUI 判断是否需要刷新
    models_version: u64,
    logs: VecDeque<LogEntry>,
    /// 每次追加日志时递增，供 GUI 增量刷新判断
    log_serial: u64,
    max_logs: usize,
    config_path: Option<PathBuf>,
    command_tx: Option<tokio::sync::mpsc::UnboundedSender<ModelCommand>>,
    recording_active: bool,
    app_config: Option<Arc<tokio::sync::Mutex<AppConfig>>>,
    config_version: u64,
    /// 配置字段 → 写入代数，用于合并连续防抖写入
    config_write_generations: HashMap<String, u64>,
}

impl SharedGuiState {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SharedGuiStateInner {
                models: Vec::new(),
                models_version: 0,
                logs: VecDeque::new(),
                log_serial: 0,
                max_logs: 100_000,
                config_path: None,
                command_tx: None,
                recording_active: false,
                app_config: None,
                config_version: 0,
                config_write_generations: HashMap::new(),
            })),
        }
    }

    /// 设置配置文件路径（用于保存开关状态）
    pub fn set_config_path(&self, path: PathBuf) {
        self.inner.lock().unwrap().config_path = Some(path);
    }

    pub fn get_config_path(&self) -> Option<PathBuf> {
        self.inner.lock().unwrap().config_path.clone()
    }

    pub fn config_version(&self) -> u64 {
        self.inner.lock().unwrap().config_version
    }

    /// 切换配置文件：停止录制 → 重载配置 → 重建模型列表
    pub fn switch_config(&self, new_path: &std::path::Path) -> anyhow::Result<()> {
        // 0. 从新路径加载配置（在锁外完成，避免 I/O 阻塞 GUI）
        let new_config = AppConfig::from_file(new_path)?;

        // 1. 停止所有录制，并取出 app_config 的 Arc（短暂持锁）
        let app_config = {
            let mut inner = self.inner.lock().unwrap();
            inner.recording_active = false;
            if let Some(ref tx) = inner.command_tx {
                let _ = tx.send(ModelCommand::StopAll);
            }
            inner
                .app_config
                .as_ref()
                .expect("app_config not set; call set_app_config() during initialization")
                .clone()
        };

        // 2. 更新 shared_app_config（锁外等待，避免持锁期间被录制回调阻塞）
        *tokio::task::block_in_place(|| app_config.blocking_lock()) = new_config.clone();

        // 3. 更新 config_path / version，并重建模型列表
        let mut inner = self.inner.lock().unwrap();
        inner.config_path = Some(new_path.to_path_buf());
        inner.config_version += 1;

        let entries = new_config.get_model_entries();
        inner.models = entries
            .iter()
            .map(|e| ModelStatus {
                username: e.username.clone(),
                enabled: e.enabled,
                status: ModelStreamStatus::Unknown,
                is_recording: false,
                recording_start: None,
                last_check: None,
                file_path: None,
            })
            .collect();
        inner.models_version += 1;

        Ok(())
    }

    /// 设置命令发送器（用于通知管理器启停模特）
    pub fn set_command_sender(&self, tx: tokio::sync::mpsc::UnboundedSender<ModelCommand>) {
        self.inner.lock().unwrap().command_tx = Some(tx);
    }

    pub fn set_app_config(&self, config: Arc<tokio::sync::Mutex<AppConfig>>) {
        self.inner.lock().unwrap().app_config = Some(config);
    }

    pub fn with_app_config<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&AppConfig) -> R,
    {
        let app_config = {
            let inner = self.inner.lock().unwrap();
            inner
                .app_config
                .as_ref()
                .expect("app_config not set; call set_app_config() during initialization")
                .clone()
        };
        let config = tokio::task::block_in_place(|| app_config.blocking_lock());
        f(&config)
    }

    pub fn with_app_config_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut AppConfig) -> R,
    {
        let app_config = {
            let inner = self.inner.lock().unwrap();
            inner
                .app_config
                .as_ref()
                .expect("app_config not set; call set_app_config() during initialization")
                .clone()
        };
        let mut config = tokio::task::block_in_place(|| app_config.blocking_lock());
        f(&mut config)
    }

    /// 设置 / 初始化模特列表
    pub fn init_models(&self, entries: &[ModelEntry]) {
        let mut inner = self.inner.lock().unwrap();
        inner.models = entries
            .iter()
            .map(|e| ModelStatus {
                username: e.username.clone(),
                enabled: e.enabled,
                status: ModelStreamStatus::Unknown,
                is_recording: false,
                recording_start: None,
                last_check: None,
                file_path: None,
            })
            .collect();
        inner.models_version += 1;
    }

    /// 更新某个模特的直播状态
    pub fn update_status(&self, username: &str, status: ModelStreamStatus) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(m) = inner.models.iter_mut().find(|m| m.username == username) {
            m.status = status;
            m.last_check = Some(Local::now());
            inner.models_version += 1;
        }
    }

    /// 标记模特开始录制
    pub fn set_recording(&self, username: &str, recording: bool, file_path: Option<String>) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(m) = inner.models.iter_mut().find(|m| m.username == username) {
            m.is_recording = recording;
            if recording {
                m.recording_start = Some(Local::now());
                m.file_path = file_path;
            } else {
                m.recording_start = None;
                m.file_path = None;
            }
            inner.models_version += 1;
        }
    }

    /// 追加一条日志
    pub fn push_log(&self, level: LogLevel, message: String) {
        let mut inner = self.inner.lock().unwrap();
        let entry = LogEntry {
            timestamp: Local::now(),
            level,
            message,
        };
        if inner.logs.len() >= inner.max_logs {
            inner.logs.pop_front();
        }
        inner.logs.push_back(entry);
        inner.log_serial = inner.log_serial.wrapping_add(1);
    }

    /// 日志版本号：每次追加日志时递增（用于 GUI 增量刷新，避免全量克隆）
    pub fn log_version(&self) -> u64 {
        self.inner.lock().unwrap().log_serial
    }

    /// 模特数据版本号：任何模特数据变化时递增（用于 GUI 判断是否需要刷新）
    pub fn models_version(&self) -> u64 {
        self.inner.lock().unwrap().models_version
    }

    /// 记录一次待执行的配置写入，返回本次写入的代数
    /// 同一字段连续多次调用时，只有最后一代会被真正写入文件
    pub fn begin_config_write(&self, key: &str) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        let entry = inner
            .config_write_generations
            .entry(key.to_string())
            .or_insert(0);
        *entry += 1;
        *entry
    }

    /// 判断指定代数是否仍是最新（用于跳过过期的防抖写入）
    pub fn is_latest_config_write(&self, key: &str, generation: u64) -> bool {
        self.inner
            .lock()
            .unwrap()
            .config_write_generations
            .get(key)
            .copied()
            == Some(generation)
    }

    /// 获取所有模特快照
    pub fn get_models(&self) -> Vec<ModelStatus> {
        self.inner.lock().unwrap().models.clone()
    }

    /// 切换模特启用状态，发送启停命令并同步到配置文件
    pub fn set_model_enabled(&self, username: &str, enabled: bool) {
        {
            let mut inner = self.inner.lock().unwrap();
            if let Some(m) = inner.models.iter_mut().find(|m| m.username == username) {
                m.enabled = enabled;
                inner.models_version += 1;
            }
            // 发送启停命令到管理器
            if let Some(ref tx) = inner.command_tx {
                let cmd = if enabled {
                    ModelCommand::Enable(username.to_string())
                } else {
                    ModelCommand::Disable(username.to_string())
                };
                let _ = tx.send(cmd);
            }
        }
        // 同步到配置文件（I/O 在锁外执行）
        self.sync_config();
    }

    /// 获取所有日志快照
    pub fn get_logs(&self) -> Vec<LogEntry> {
        self.inner.lock().unwrap().logs.iter().cloned().collect()
    }

    /// 新增模特
    pub fn add_model(&self, username: &str, enabled: bool) {
        {
            let mut inner = self.inner.lock().unwrap();
            // 检查是否已存在
            if inner.models.iter().any(|m| m.username == username) {
                return;
            }
            inner.models.push(ModelStatus {
                username: username.to_string(),
                enabled,
                status: ModelStreamStatus::Unknown,
                is_recording: false,
                recording_start: None,
                last_check: None,
                file_path: None,
            });
            inner.models_version += 1;
            // 发送命令到管理器
            if enabled {
                if let Some(ref tx) = inner.command_tx {
                    let _ = tx.send(ModelCommand::Add(username.to_string(), true));
                }
            }
        }
        self.sync_config();
    }

    /// 获取总录制开关状态
    pub fn is_recording_active(&self) -> bool {
        self.inner.lock().unwrap().recording_active
    }

    /// 设置总录制开关
    pub fn set_recording_active(&self, active: bool) {
        let mut inner = self.inner.lock().unwrap();
        inner.recording_active = active;
        if let Some(ref tx) = inner.command_tx {
            let cmd = if active {
                ModelCommand::StartAll
            } else {
                ModelCommand::StopAll
            };
            let _ = tx.send(cmd);
        }
    }

    /// 删除模特
    pub fn remove_model(&self, username: &str) {
        {
            let mut inner = self.inner.lock().unwrap();
            // 发送移除命令（管理器会停止录制）
            if let Some(ref tx) = inner.command_tx {
                let _ = tx.send(ModelCommand::Remove(username.to_string()));
            }
            inner.models.retain(|m| m.username != username);
            inner.models_version += 1;
        }
        self.sync_config();
    }

    /// 上移/下移模特（手动排序）：delta 为 -1 表示上移，1 表示下移
    pub fn move_model(&self, username: &str, delta: i32) {
        {
            let mut inner = self.inner.lock().unwrap();
            let Some(ix) = inner.models.iter().position(|m| m.username == username) else {
                return;
            };
            let len = inner.models.len() as i32;
            let new_ix = (ix as i32 + delta).clamp(0, len - 1) as usize;
            if new_ix == ix {
                return;
            }
            let m = inner.models.remove(ix);
            inner.models.insert(new_ix, m);
            inner.models_version += 1;
        }
        // 排序结果同步到配置文件（保持 usernames 数组顺序）
        self.sync_config();
    }

    /// 同步模特列表到配置文件（I/O 与锁等待均在锁外执行，避免阻塞 GUI）
    fn sync_config(&self) {
        // 锁内只收集数据，不执行任何 I/O
        let (config_path, app_config, entries) = {
            let inner = self.inner.lock().unwrap();
            let config_path = inner.config_path.clone();
            let app_config = inner.app_config.clone();
            let entries: Vec<serde_json::Value> = inner
                .models
                .iter()
                .map(|m| {
                    serde_json::json!({
                        "username": m.username,
                        "enabled": m.enabled
                    })
                })
                .collect();
            (config_path, app_config, entries)
        };

        let Some(ref config_path) = config_path else {
            return;
        };

        // 如果配置文件不存在，先写入完整默认配置（锁外 I/O）
        if !config_path.exists() {
            if let Some(ref app_config) = app_config {
                let config = tokio::task::block_in_place(|| app_config.blocking_lock());
                let _ = config.save_to_file(config_path);
            }
        }

        let _ =
            AppConfig::update_field(config_path, "usernames", serde_json::Value::Array(entries));
    }
}
