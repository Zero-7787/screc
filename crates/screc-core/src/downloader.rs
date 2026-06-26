use anyhow::{Result, anyhow};
use log::{debug, error, info, warn};
use reqwest::Client;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::broadcast;
use url::Url;

/// 每 N 个分片后执行一次 flush（减少磁盘同步开销）
const FLUSH_INTERVAL: usize = 10;
/// 空播放列表连续出现多少次后认为直播结束
const MAX_EMPTY_PLAYLISTS: u32 = 10;
/// 单个分片最大失败次数（跨多轮播放列表），超过后放弃该分片
const MAX_SEGMENT_FAILURES: u32 = 5;
/// 单个分片单次下载超时（秒）
const SEGMENT_DOWNLOAD_TIMEOUT_SECS: u64 = 30;
/// 有效分片的最小字节数（低于此值视为无效数据）
const MIN_SEGMENT_SIZE: usize = 128;
/// 连续 PendingRetry 轮次上限，防止 CDN 全面故障时无限自旋
const MAX_CONSECUTIVE_PENDING_RETRIES: u32 = 20;

/// 每轮播放列表处理的结果分类
/// 用于主循环正确区分"真没新分片"和"有新分片但暂时下载失败"
#[derive(Debug, PartialEq)]
enum RoundOutcome {
    /// 至少有一个分片成功下载并写入磁盘
    DownloadedContent,
    /// 播放列表中确实没有新分片（全部 URI 已知）
    NoNewSegments,
    /// 播放列表中有新分片但全部下载失败（将在下轮重试，不计入空轮计数）
    PendingRetry,
}

pub struct HlsDownloader {
    client: Client,                                // HTTP客户端
    downloaded_segments: HashSet<String>,          // 已成功写入的分片集合
    failed_segment_attempts: HashMap<String, u32>, // 失败分片的跨轮重试计数
    init_segment_downloaded: bool,                 // 是否已下载初始化分片
    shutdown_rx: Option<broadcast::Receiver<()>>,  // 关闭信号接收器
    username: String,                              // 用户名
    total_processed_segments: usize,               // 已处理的分片总数
    segments_since_flush: usize,                   // 自上次 flush 以来的分片数
}

impl HlsDownloader {
    /// 创建新的 HLS 下载器
    pub fn new(client: Client, username: String) -> Self {
        Self {
            client,
            downloaded_segments: HashSet::new(),
            failed_segment_attempts: HashMap::new(),
            init_segment_downloaded: false,
            shutdown_rx: None,
            username,
            total_processed_segments: 0,
            segments_since_flush: 0,
        }
    }

    /// 添加关闭信号接收器
    pub fn with_shutdown_receiver(mut self, shutdown_rx: broadcast::Receiver<()>) -> Self {
        self.shutdown_rx = Some(shutdown_rx);
        self
    }

    /// 检查是否收到关闭信号
    /// 返回 true 表示应该停止，false 表示继续运行
    fn check_shutdown_signal(&mut self) -> bool {
        if let Some(ref mut shutdown_rx) = self.shutdown_rx {
            match shutdown_rx.try_recv() {
                Ok(_) => {
                    info!("[{}] 下载器收到关闭信号，停止下载", self.username);
                    true
                }
                Err(broadcast::error::TryRecvError::Empty) => false,
                Err(broadcast::error::TryRecvError::Closed) => {
                    info!("[{}] 下载器关闭信号通道已关闭，停止下载", self.username);
                    true
                }
                Err(broadcast::error::TryRecvError::Lagged(_)) => {
                    info!("[{}] 下载器错过了关闭信号，停止下载", self.username);
                    true
                }
            }
        } else {
            false
        }
    }

    /// 可中断的等待函数
    /// 返回 true 表示收到关闭信号应该停止，false 表示等待完成可以继续
    async fn interruptible_sleep(&mut self, duration: tokio::time::Duration) -> bool {
        if let Some(ref mut shutdown_rx) = self.shutdown_rx {
            let mut shutdown_rx = shutdown_rx.resubscribe();
            tokio::select! {
                _ = tokio::time::sleep(duration) => {
                    self.check_shutdown_signal()
                }
                _ = shutdown_rx.recv() => {
                    info!("[{}] 下载器等待期间收到关闭信号，停止下载", self.username);
                    true
                }
            }
        } else {
            tokio::time::sleep(duration).await;
            false
        }
    }

    /// 下载 HLS 流并实时转换为真正的 MPEG-TS 流（支持按时无缝切片）
    pub async fn download_hls_stream<F>(
        &mut self,
        playlist_url: &str,
        output_path: &Path,
        m3u_processor: Option<&F>,
    ) -> Result<()>
    where
        F: Fn(&str) -> String,
    {
        debug!("[{}] 开始 HLS 下载，基础路径: {:?}", self.username, output_path);

        // --- 【自定义单视频最大录制时长】 ---
        let max_duration = tokio::time::Duration::from_secs(3600); // 3600秒 = 1小时
        let mut part_index = 1;
        let mut part_start_time = tokio::time::Instant::now();

        // 辅助闭包：生成当前分段真正的 TS 路径
        let get_part_ts_path = |base_path: &Path, idx: usize| {
            let stem = base_path.file_stem().unwrap_or_default().to_string_lossy();
            let parent = base_path.parent().unwrap_or_else(|| Path::new(""));
            parent.join(format!("{}_part{}.ts", stem, idx))
        };

        // 辅助函数：启动实时将标准输入转换为标准 MPEG-TS 写入文件的 FFmpeg 进程
        let spawn_ffmpeg_muxer = |ts_path: &Path| -> Result<(tokio::process::Child, tokio::process::ChildStdin)> {
            use std::process::Stdio;
            let mut std_cmd = std::process::Command::new("ffmpeg");
            std_cmd
                .arg("-loglevel").arg("error")
                .arg("-fflags").arg("+genpts+igndts")
                .arg("-i").arg("pipe:0") 
                .arg("-c").arg("copy")   
                .arg("-f").arg("mpegts") 
                .arg("-y")
                .arg(ts_path);

            #[cfg(windows)]
            {
                use std::os::windows::process::CommandExt;
                std_cmd.creation_flags(0x08000000); // CREATE_NO_WINDOW
            }

            let mut cmd = tokio::process::Command::from(std_cmd);
            cmd.stdin(Stdio::piped());

            let mut child = cmd.spawn().map_err(|e| anyhow!("无法启动实时 FFmpeg 转换器: {}", e))?;
            let stdin = child.stdin.take().ok_or_else(|| anyhow!("无法打开 FFmpeg 写入管道"))?;
            Ok((child, stdin))
        };

        // 初始化第一个分段的真正的 TS 文件和 FFmpeg 管道
        let mut current_ts_path = get_part_ts_path(output_path, part_index);
        let (mut ffmpeg_child, ffmpeg_stdin) = spawn_ffmpeg_muxer(&current_ts_path)?;
        let mut output_file = BufWriter::with_capacity(256 * 1024, ffmpeg_stdin);
        
        let mut has_downloaded_content = false;
        let mut consecutive_empty_playlists = 0u32;
        let mut consecutive_pending_retries = 0u32;

        let calc_wait_time = |target_duration: u64| -> u64 {
            if target_duration <= 2 { 1 } else if target_duration <= 6 { target_duration / 2 } else { 3 }
        };

        let mut shutdown_rx = self.shutdown_rx.as_mut().map(|rx| rx.resubscribe());

        let download_result = 'main: loop {
            let round_result = if let Some(ref mut rx) = shutdown_rx {
                tokio::select! {
                    r = self.download_playlist_segments(playlist_url, &mut output_file, m3u_processor) => r,
                    _ = rx.recv() => {
                        info!("[{}] 收到关闭信号，停止下载新分片", self.username);
                        break 'main Ok(());
                    }
                }
            } else {
                self.download_playlist_segments(playlist_url, &mut output_file, m3u_processor).await
            };

            match round_result {
                Ok((RoundOutcome::DownloadedContent, _)) => {
                    has_downloaded_content = true;
                    consecutive_empty_playlists = 0;
                    consecutive_pending_retries = 0;

                    if part_start_time.elapsed() >= max_duration {
                        info!("[{}] 当前分段已满 {} 秒，正在无缝切换到下一 TS 分段...", self.username, max_duration.as_secs());

                        if let Err(e) = output_file.flush().await {
                            error!("[{}] 刷新 FFmpeg 管道失败: {}", self.username, e);
                        }
                        drop(output_file);
                        let _ = ffmpeg_child.wait().await;

                        part_index += 1;
                        current_ts_path = get_part_ts_path(output_path, part_index);
                        
                        let (next_child, next_stdin) = spawn_ffmpeg_muxer(&current_ts_path)?;
                        ffmpeg_child = next_child;
                        output_file = BufWriter::with_capacity(256 * 1024, next_stdin);
                        
                        part_start_time = tokio::time::Instant::now();
                    }
                    continue;
                }
                Ok((RoundOutcome::NoNewSegments, target_duration)) => {
                    consecutive_pending_retries = 0;
                    consecutive_empty_playlists += 1;
                    if consecutive_empty_playlists >= MAX_EMPTY_PLAYLISTS {
                        info!("[{}] 直播可能已结束", self.username);
                        break 'main Ok(());
                    }
                    let wait_time = calc_wait_time(target_duration);
                    if self.interruptible_sleep(tokio::time::Duration::from_secs(wait_time)).await {
                        break 'main Ok(());
                    }
                }
                Ok((RoundOutcome::PendingRetry, _)) => {
                    consecutive_pending_retries += 1;
                    if consecutive_pending_retries >= MAX_CONSECUTIVE_PENDING_RETRIES {
                        break 'main Ok(());
                    }
                    if self.interruptible_sleep(tokio::time::Duration::from_secs(1)).await {
                        break 'main Ok(());
                    }
                }
                Err(e) => {
                    error!("[{}] 下载轮次出错: {}", self.username, e);
                    consecutive_empty_playlists += 1;
                    if consecutive_empty_playlists >= MAX_EMPTY_PLAYLISTS {
                        break 'main Err(e);
                    }
                    if self.interruptible_sleep(tokio::time::Duration::from_secs(3)).await {
                        break 'main Ok(());
                    }
                }
            }
        };

        if let Err(e) = output_file.flush().await {
            error!("[{}] 最终刷新 FFmpeg 管道失败: {}", self.username, e);
        }
        drop(output_file);
        let _ = ffmpeg_child.wait().await;

        if has_downloaded_content {
            info!("[{}] 所有分段已实时封装为纯正的 MPEG-TS 流！", self.username);
        } else {
            let _ = tokio::fs::remove_file(&current_ts_path).await;
        }

        download_result
    }

    /// 下载播放列表分片（泛型 W 支持任意异步写入目标）
    async fn download_playlist_segments<F, W>(
        &mut self,
        playlist_url: &str,
        output_file: &mut BufWriter<W>,
        m3u_processor: Option<&F>,
    ) -> Result<(RoundOutcome, u64)>
    where
        F: Fn(&str) -> String,
        W: tokio::io::AsyncWrite + Unpin,
    {
        debug!("[{}] 获取播放列表: {}", self.username, playlist_url);

        let response = self
            .client
            .get(playlist_url)
            .header("Accept", "*/*")
            .header("Accept-Language", "en-US,en;q=0.5")
            .header("DNT", "1")
            .header("Connection", "keep-alive")
            .header("Sec-Fetch-Dest", "empty")
            .header("Sec-Fetch-Mode", "cors")
            .header("Sec-Fetch-Site", "cross-site")
            .send()
            .await?;
        if !response.status().is_success() {
            return Err(anyhow!("获取播放列表失败: {}", response.status()));
        }

        let mut content = response.text().await?;

        if let Some(processor) = m3u_processor {
            content = processor(&content);
        }

        let mut target_duration = 6u64;
        let mut init_failed = false; 
        let mut init_just_downloaded = false; 

        for line in content.lines() {
            if line.starts_with("#EXT-X-TARGETDURATION:") {
                if let Some(duration_str) = line.strip_prefix("#EXT-X-TARGETDURATION:") {
                    if let Ok(duration) = duration_str.parse::<u64>() {
                        target_duration = duration;
                        break;
                    }
                }
            }
        }

        if !self.init_segment_downloaded {
            if let Some(init_url) = self.extract_init_segment(&content, playlist_url)? {
                debug!("[{}] 下载初始化分片: {}", self.username, init_url);
                match self.download_with_retry(&init_url).await {
                    Ok(data) => {
                        output_file.write_all(&data).await?;
                        self.init_segment_downloaded = true;
                        init_just_downloaded = true;
                    }
                    Err(e) => {
                        error!("[{}] 初始化分片下载失败: {}", self.username, e);
                        init_failed = true;
                    }
                }
            }
        }

        let playlist = m3u8_rs::parse_playlist_res(content.as_bytes())
            .map_err(|e| anyhow!("解析 M3U8 失败: {:?}", e))?;

        match playlist {
            m3u8_rs::Playlist::MediaPlaylist(media_playlist) => {
                let base_url = Url::parse(playlist_url)?;

                let new_segments: Vec<(String, String)> = media_playlist
                    .segments
                    .iter()
                    .filter(|seg| !self.downloaded_segments.contains(&seg.uri))
                    .map(|seg| {
                        let url = if seg.uri.starts_with("http") {
                            seg.uri.clone()
                        } else {
                            base_url
                                .join(&seg.uri)
                                .map(|u| u.to_string())
                                .unwrap_or_else(|_| seg.uri.clone())
                        };
                        (url, seg.uri.clone())
                    })
                    .collect();

                if new_segments.is_empty() {
                    let outcome = if init_failed {
                        RoundOutcome::PendingRetry
                    } else if init_just_downloaded {
                        RoundOutcome::DownloadedContent
                    } else {
                        RoundOutcome::NoNewSegments
                    };
                    return Ok((outcome, target_duration));
                }

                if init_failed {
                    return Ok((RoundOutcome::PendingRetry, target_duration));
                }

                let new_count = new_segments.len();
                let mut any_succeeded = false;
                let mut any_failed_pending = false;
                let total_visible = self.total_processed_segments + new_count;

                for (seg_url, seg_uri) in &new_segments {
                    let result = self.download_with_retry(seg_url).await;

                    match &result {
                        Ok(data) if data.is_empty() => {
                            self.downloaded_segments.insert(seg_uri.clone());
                            self.failed_segment_attempts.remove(seg_uri);
                        }
                        Ok(data) => {
                            info!(
                                "[{}] 正在处理分片 {}/{} ({} 字节)",
                                self.username,
                                self.total_processed_segments + 1,
                                total_visible,
                                data.len()
                            );
                            output_file.write_all(data).await?;
                            self.total_processed_segments += 1;
                            self.segments_since_flush += 1;
                            if self.segments_since_flush >= FLUSH_INTERVAL {
                                output_file.flush().await?;
                                self.segments_since_flush = 0;
                            }
                            self.downloaded_segments.insert(seg_uri.clone());
                            self.failed_segment_attempts.remove(seg_uri);
                            any_succeeded = true;
                        }
                        Err(e) => {
                            let attempts = self
                                .failed_segment_attempts
                                .entry(seg_uri.clone())
                                .and_modify(|c| *c += 1)
                                .or_insert(1);

                            if *attempts >= MAX_SEGMENT_FAILURES {
                                self.downloaded_segments.insert(seg_uri.clone());
                                self.failed_segment_attempts.remove(seg_uri);
                            } else {
                                any_failed_pending = true;
                                break;
                            }
                        }
                    }
                }

                let outcome = if any_succeeded {
                    RoundOutcome::DownloadedContent
                } else if any_failed_pending {
                    RoundOutcome::PendingRetry
                } else {
                    RoundOutcome::NoNewSegments
                };

                return Ok((outcome, target_duration));
            }
            m3u8_rs::Playlist::MasterPlaylist(_) => {
                return Err(anyhow!("此上下文不支持主播放列表"));
            }
        }
    }

    /// 带重试和超时的单个分片下载
    async fn download_with_retry(&self, segment_url: &str) -> Result<Vec<u8>> {
        const MAX_RETRIES: u32 = 3;
        let mut last_error = None;

        for attempt in 0..MAX_RETRIES {
            let download_future = self.try_download_segment_data(segment_url);

            match tokio::time::timeout(
                tokio::time::Duration::from_secs(SEGMENT_DOWNLOAD_TIMEOUT_SECS),
                download_future,
            )
            .await
            {
                Ok(Ok(data)) => return Ok(data),
                Ok(Err(e)) => last_error = Some(e),
                Err(_elapsed) => {
                    last_error = Some(anyhow!("分片下载超时 ({}秒)", SEGMENT_DOWNLOAD_TIMEOUT_SECS));
                }
            }

            if attempt + 1 < MAX_RETRIES {
                let delay_ms = 500 * (attempt + 1) as u64;
                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
            }
        }

        Err(last_error.unwrap())
    }

    /// 下载单个分片的原始数据
    async fn try_download_segment_data(&self, segment_url: &str) -> Result<Vec<u8>> {
        let response = self
            .client
            .get(segment_url)
            .header("Accept", "*/*")
            .header("Accept-Language", "en-US,en;q=0.5")
            .header("DNT", "1")
            .header("Connection", "keep-alive")
            .header("Sec-Fetch-Dest", "empty")
            .header("Sec-Fetch-Mode", "cors")
            .header("Sec-Fetch-Site", "cross-site")
            .send()
            .await?;

        match response.status() {
            reqwest::StatusCode::OK => {}
            reqwest::StatusCode::IM_A_TEAPOT => return Err(anyhow!("分片尚未就绪 (418)，将重试")),
            reqwest::StatusCode::NOT_FOUND => return Ok(Vec::new()),
            reqwest::StatusCode::FORBIDDEN => return Ok(Vec::new()),
            reqwest::StatusCode::TOO_MANY_REQUESTS => return Err(anyhow!("请求过于频繁 (429)，将重试")),
            status => return Err(anyhow!("下载分片失败: {} {}", status.as_u16(), status.canonical_reason().unwrap_or("未知"))),
        }

        let content_type = response.headers().get(reqwest::header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or("");
        if content_type.to_lowercase().contains("text/html") {
            return Err(anyhow!("分片返回了 HTML 而非视频数据"));
        }

        let expected_len = response.headers().get(reqwest::header::CONTENT_LENGTH).and_then(|v| v.to_str().ok()).and_then(|s| s.parse::<usize>().ok());
        let bytes = response.bytes().await?;
        let actual_len = bytes.len();

        if actual_len < MIN_SEGMENT_SIZE {
            return Err(anyhow!("分片数据过小: {} 字节", actual_len));
        }

        if let Some(expected) = expected_len {
            if actual_len != expected {
                return Err(anyhow!("分片数据不完整: 期望 {} 字节，实际 {} 字节", expected, actual_len));
            }
        }

        Ok(bytes.to_vec())
    }

    /// 提取初始化分片 URL
    fn extract_init_segment(&self, content: &str, playlist_url: &str) -> Result<Option<String>> {
        content
            .lines()
            .find(|line| line.starts_with("#EXT-X-MAP:URI="))
            .and_then(|line| {
                line.find("URI=\"")
                    .map(|start| start + 5)
                    .and_then(|start| line[start..].find('\"').map(|end| &line[start..start + end]))
            })
            .map(|init_uri| {
                let init_url = if init_uri.starts_with("http") {
                    init_uri.to_string()
                } else {
                    let base_url = Url::parse(playlist_url)?;
                    base_url.join(init_uri)?.to_string()
                };
                Ok(init_url)
            })
            .transpose()
    }
}
