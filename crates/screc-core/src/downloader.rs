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

    /// 下载 HLS 流（统一循环，录制完成后自动触发转码）
    pub async fn download_hls_stream<F>(
        &mut self,
        playlist_url: &str,
        output_path: &Path,
        m3u_processor: Option<&F>,
    ) -> Result<()>
    where
        F: Fn(&str) -> String,
    {
        debug!("[{}] 开始 HLS 下载到: {:?}", self.username, output_path);

        // 为原始 MP4/TS 分片创建临时文件，使用 BufWriter 减少系统调用
        let temp_path = output_path.with_extension("tmp.mp4");
        let file = File::create(&temp_path).await?;
        let mut output_file = BufWriter::with_capacity(256 * 1024, file); 
        let mut has_downloaded_content = false;
        let mut consecutive_empty_playlists = 0u32;
        let mut consecutive_pending_retries = 0u32; 

        // 计算动态等待时间的辅助函数
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
                    continue;
                }
                Ok((RoundOutcome::NoNewSegments, target_duration)) => {
                    consecutive_pending_retries = 0;
                    consecutive_empty_playlists += 1;
                    if consecutive_empty_playlists >= MAX_EMPTY_PLAYLISTS {
                        info!("[{}] 连续 {} 次未发现新分片，直播可能已结束", self.username, MAX_EMPTY_PLAYLISTS);
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

        // 确保缓冲数据写入磁盘，并必须提前关闭释放临时文件锁
        if let Err(e) = output_file.flush().await {
            error!("[{}] 刷新文件缓冲区失败: {}", self.username, e);
        }
        drop(output_file); 

        // 【确保触发点】：实际拉到内容后，立即调用 FFmpeg 进行自动无损转码封装
        if has_downloaded_content {
            debug!("[{}] 正在将录制内容转换为标准的 MP4 格式...", self.username);
            match self.convert_ts_to_mp4(&temp_path, output_path).await {
                Ok(()) => {
                    info!("[{}] 视频转换成功完成，录制已保存至: {:?}", self.username, output_path);
                    if let Err(e) = tokio::fs::remove_file(&temp_path).await {
                        error!("[{}] 清理临时文件失败: {}", self.username, e);
                    }
                }
                Err(e) => {
                    error!("[{}] 视频转换失败: {}", self.username, e);
                    warn!("[{}] 原始数据已保留在: {:?}，可手动用 ffmpeg 修复", self.username, temp_path);
                }
            }
        } else {
            debug!("[{}] 没有下载任何内容，跳过视频转换", self.username);
            let _ = tokio::fs::remove_file(&temp_path).await;
        }

        download_result
    }

    /// 下载播放列表分片
    async fn download_playlist_segments<F>(
        &mut self,
        playlist_url: &str,
        output_file: &mut BufWriter<File>,
        m3u_processor: Option<&F>,
    ) -> Result<(RoundOutcome, u64)>
    where
        F: Fn(&str) -> String,
    {
        let response = self
            .client
            .get(playlist_url)
            .header("Accept", "*/*")
            .header("Connection", "keep-alive")
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
            if let Some(duration_str) = line.strip_prefix("#EXT-X-TARGETDURATION:") {
                if let Ok(duration) = duration_str.parse::<u64>() {
                    target_duration = duration;
                    break;
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
            .send()
            .await?;

        match response.status() {
            reqwest::StatusCode::OK => {}
            reqwest::StatusCode::IM_A_TEAPOT => return Err(anyhow!("分片尚未就绪 (418)，将重试")),
            reqwest::StatusCode::NOT_FOUND => return Ok(Vec::new()),
            reqwest::StatusCode::FORBIDDEN => return Ok(Vec::new()),
            reqwest::StatusCode::TOO_MANY_REQUESTS => return Err(anyhow!("请求过于频繁 (429)，将重试")),
            status => return Err(anyhow!("下载分片失败: {}", status.as_u16())),
        }

        let bytes = response.bytes().await?;
        let actual_len = bytes.len();

        if actual_len < MIN_SEGMENT_SIZE {
            return Err(anyhow!("分片数据过小: {} 字节", actual_len));
        }

        Ok(bytes.to_vec())
    }

    /// 使用 FFmpeg 将 fMP4/TS 转换为 MP4
    async fn convert_ts_to_mp4(&self, input_path: &Path, output_path: &Path) -> Result<()> {
        use std::process::Command;

        let mut std_cmd = Command::new("ffmpeg");
        std_cmd
            .arg("-fflags")
            .arg("+genpts+igndts")
            .arg("-i")
            .arg(input_path)
            .arg("-c:a")
            .arg("copy")
            .arg("-c:v")
            .arg("copy")
            .arg("-movflags")
            .arg("+faststart")
            .arg("-y")
            .arg(output_path);

        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            std_cmd.creation_flags(0x08000000 | 0x00000008);
        }

        let mut cmd = tokio::process::Command::from(std_cmd);
        let output = cmd.output().await.map_err(|e| {
            anyhow!("运行 FFmpeg 失败: {}。请确保 FFmpeg 已安装并在 PATH 中。", e)
        })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("FFmpeg 转换失败: {}", stderr));
        }

        Ok(())
    }

    /// 提取初始化分片 URL
    fn extract_init_segment(&self, content: &str, playlist_url: &str) -> Result<Option<String>> {
        content
            .lines()
            .find(|line| line.starts_with("#EXT-X-MAP:URI="))
            .and_then(|line| {
                line.find("URI=\"")
                    .map(|start| start + 5)
                    .and_then(|start| line[start..].find('"').map(|end| &line[start..start + end]))
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
