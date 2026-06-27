use anyhow::{Result, anyhow};
use log::{debug, error, info, warn};
use reqwest::Client;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::{Duration, Instant};
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::broadcast;
use url::Url;

const FLUSH_INTERVAL: usize = 10;
const MAX_EMPTY_PLAYLISTS: u32 = 10;
const MAX_SEGMENT_FAILURES: u32 = 5;
const SEGMENT_DOWNLOAD_TIMEOUT_SECS: u64 = 30;
const MIN_SEGMENT_SIZE: usize = 128;
const MAX_CONSECUTIVE_PENDING_RETRIES: u32 = 20;
const MAX_CONSECUTIVE_FAILED_SEGMENTS: u32 = 3; 

#[derive(Debug, PartialEq)]
enum RoundOutcome {
    DownloadedContent,
    NoNewSegments,
    PendingRetry,
    FatalSegmentError, 
    TicketedRoom,      
}

enum SessionEndReason {
    MaxDurationReached,
    FatalSegmentError,
    StreamEnded,
    Shutdown,
    TicketedRoom, // 【新增】门票房间退出状态
}

pub struct HlsDownloader {
    client: Client,
    downloaded_segments: HashSet<String>,
    failed_segment_attempts: HashMap<String, u32>,
    init_segment_downloaded: bool,
    shutdown_rx: Option<broadcast::Receiver<()>>,
    username: String,
    total_processed_segments: usize,
    segments_since_flush: usize,
}

impl HlsDownloader {
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

    pub fn with_shutdown_receiver(mut self, shutdown_rx: broadcast::Receiver<()>) -> Self {
        self.shutdown_rx = Some(shutdown_rx);
        self
    }

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

        let mut part_index = 1;
        let max_duration = Duration::from_secs(60 * 60); 

        'record_session: loop {
            let final_mp4_path = if part_index == 1 {
                output_path.with_extension("mp4")
            } else {
                let file_stem = output_path.file_stem().unwrap_or_default().to_string_lossy();
                output_path.with_file_name(format!("{}_part{}.mp4", file_stem, part_index))
            };

            let temp_ts_path = output_path.with_extension(format!("tmp{}.ts", part_index));
            let file = File::create(&temp_ts_path).await?;
            let mut output_file = BufWriter::with_capacity(256 * 1024, file); 
            let mut has_downloaded_content = false;
            let mut consecutive_empty_playlists = 0u32;
            let mut consecutive_pending_retries = 0u32; 
            
            let start_time = Instant::now();

            let calc_wait_time = |target_duration: u64| -> u64 {
                if target_duration <= 2 { 1 } else if target_duration <= 6 { target_duration / 2 } else { 3 }
            };

            let mut shutdown_rx = self.shutdown_rx.as_mut().map(|rx| rx.resubscribe());

            let session_end_reason = 'poll_loop: loop {
                if start_time.elapsed() >= max_duration {
                    info!("[{}] 当前录制已达 60 分钟，准备自动切断并生成当前分段...", self.username);
                    break 'poll_loop SessionEndReason::MaxDurationReached; 
                }

                let round_result = if let Some(ref mut rx) = shutdown_rx {
                    tokio::select! {
                        r = self.download_playlist_segments(playlist_url, &mut output_file, m3u_processor) => r,
                        _ = rx.recv() => {
                            info!("[{}] 收到关闭信号，停止下载新分片，准备开始收尾转码...", self.username);
                            break 'poll_loop SessionEndReason::Shutdown;  
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
                            info!("[{}] 连续 {} 次未发现新分片，直播正常结束", self.username, MAX_EMPTY_PLAYLISTS);
                            break 'poll_loop SessionEndReason::StreamEnded;
                        }
                        let wait_time = calc_wait_time(target_duration);
                        if self.interruptible_sleep(tokio::time::Duration::from_secs(wait_time)).await {
                            break 'poll_loop SessionEndReason::Shutdown; 
                        }
                    }
                    Ok((RoundOutcome::PendingRetry, _)) => {
                        consecutive_pending_retries += 1;
                        if consecutive_pending_retries >= MAX_CONSECUTIVE_PENDING_RETRIES {
                            warn!("[{}] 连续分片下载失败过多次，触发重连机制", self.username);
                            break 'poll_loop SessionEndReason::FatalSegmentError;
                        }
                        if self.interruptible_sleep(tokio::time::Duration::from_secs(1)).await {
                            break 'poll_loop SessionEndReason::Shutdown; 
                        }
                    }
                    Ok((RoundOutcome::FatalSegmentError, _)) => {
                        if !has_downloaded_content {
                            error!("[{}] 无法获取任何有效分片，放弃重连，彻底停止录制", self.username);
                            break 'poll_loop SessionEndReason::StreamEnded;
                        }
                        error!("[{}] 网络异常或连接断开，立即触发断点保存与重连...", self.username);
                        break 'poll_loop SessionEndReason::FatalSegmentError;
                    }
                    Ok((RoundOutcome::TicketedRoom, _)) => {
                        // 【核心修复】：不再直接 return Err，而是跳出循环，去执行下面的转码封包！
                        error!("[{}] 检测到 403 被拒绝访问，主播开启了门票/付费限制，准备保存进度并停止录制", self.username);
                        break 'poll_loop SessionEndReason::TicketedRoom;
                    }
                    Err(e) => {
                        error!("[{}] 获取播放列表异常: {}", self.username, e);
                        consecutive_empty_playlists += 1; 
                        
                        if consecutive_empty_playlists >= MAX_EMPTY_PLAYLISTS {
                            warn!("[{}] 连续 {} 次获取播放列表失败 (可能已下播)，正常结束录制", self.username, MAX_EMPTY_PLAYLISTS);
                            break 'poll_loop SessionEndReason::StreamEnded;
                        }
                        
                        if self.interruptible_sleep(tokio::time::Duration::from_secs(3)).await {
                            break 'poll_loop SessionEndReason::Shutdown; 
                        }
                    }
                }
            };

            // 循环结束，不论是什么原因，都会执行这里的缓冲区刷新和转码！
            if let Err(e) = output_file.flush().await {
                error!("[{}] 刷新文件缓冲区失败: {}", self.username, e);
            }
            drop(output_file);

            if has_downloaded_content {
                debug!("[{}] 正在将分段 {} 封装并重整为 MP4 格式...", self.username, part_index);
                match self.convert_ts_to_mp4(&temp_ts_path, &final_mp4_path).await {
                    Ok(()) => {
                        info!("[{}] 分段 {} MP4 转换成功", self.username, part_index);
                        if let Err(e) = tokio::fs::remove_file(&temp_ts_path).await {
                            warn!("[{}] 删除临时 TS 文件失败: {}", self.username, e);
                        }
                    }
                    Err(e) => {
                        error!("[{}] 分段 {} 转码 MP4 失败: {}", self.username, part_index, e);
                        warn!("[{}] 原始数据已保留在: {:?}，可手动用 ffmpeg 修复", self.username, temp_ts_path);
                    }
                }
            } else {
                debug!("[{}] 没有下载任何内容，跳过视频转换", self.username);
                if let Err(e) = tokio::fs::remove_file(&temp_ts_path).await {
                    debug!("[{}] 清理空临时文件失败: {}", self.username, e);
                }
            }

            // 根据退出原因收尾：如果是门票限制，就在转码完成后向主控台抛出错误
            match session_end_reason {
                SessionEndReason::Shutdown => {
                    info!("[{}] 录制收尾工作完成，已彻底退出", self.username);
                    return Ok(());
                }
                SessionEndReason::StreamEnded => {
                    info!("[{}] 直播流已结束或失效，录制正常结束", self.username);
                    return Ok(());
                }
                SessionEndReason::TicketedRoom => {
                    info!("[{}] 已保存门票开启前的视频内容", self.username);
                    return Err(anyhow!("主播开启了门票/付费限制，已停止录制")); // 报错给主控台，让其切换到 Private 状态
                }
                SessionEndReason::MaxDurationReached | SessionEndReason::FatalSegmentError => {
                    part_index += 1;
                    self.init_segment_downloaded = false; 
                    continue 'record_session;
                }
            }
        }
    }

    async fn download_playlist_segments<F>(
        &mut self,
        playlist_url: &str,
        output_file: &mut BufWriter<File>,
        m3u_processor: Option<&F>,
    ) -> Result<(RoundOutcome, u64)>
    where
        F: Fn(&str) -> String,
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
            if response.status() == reqwest::StatusCode::FORBIDDEN {
                return Ok((RoundOutcome::TicketedRoom, 6)); 
            }
            return Err(anyhow!("获取播放列表失败 ({}): 该地址可能已失效", response.status()));
        }

        let mut content = response.text().await?;

        if let Some(processor) = m3u_processor {
            content = processor(&content);
        }

        let mut target_duration = 6u64;
        let mut init_failed = false; 
        let mut init_just_downloaded = false; 

        for line in content.lines() {
            if let Some(duration_str) = line.strip_prefix("#EXT-X-TARGETDURATION:")
                && let Ok(duration) = duration_str.parse::<u64>()
            {
                target_duration = duration;
                break;
            }
        }

        if !self.init_segment_downloaded
            && let Some(init_url) = self.extract_init_segment(&content, playlist_url)?
        {
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
                let mut consecutive_failed_segments = 0; 
                let total_visible = self.total_processed_segments + new_count;

                for (seg_url, seg_uri) in &new_segments {
                    let result = self.download_with_retry(seg_url).await;

                    match &result {
                        Ok(data) if data.is_empty() => {
                            self.downloaded_segments.insert(seg_uri.clone());
                            self.failed_segment_attempts.remove(seg_uri);
                        }
                        Ok(data) => {
                            consecutive_failed_segments = 0; 

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
                            consecutive_failed_segments += 1;
                            let err_msg = e.to_string();

                            if err_msg.contains("403_FORBIDDEN") {
                                warn!("[{}] 分片访问被拒绝 (403)，主播可能开启了门票/付费房间", self.username);
                                return Ok((RoundOutcome::TicketedRoom, target_duration));
                            }

                            if consecutive_failed_segments >= MAX_CONSECUTIVE_FAILED_SEGMENTS {
                                warn!("[{}] 网络状态极差，连续 {} 个分片下载失败", self.username, consecutive_failed_segments);
                                return Ok((RoundOutcome::FatalSegmentError, target_duration));
                            }

                            let attempts = self
                                .failed_segment_attempts
                                .entry(seg_uri.clone())
                                .and_modify(|c| *c += 1)
                                .or_insert(1);

                            if *attempts >= MAX_SEGMENT_FAILURES {
                                error!("[{}] 分片 {} 已失败 {} 次，放弃重试", self.username, seg_uri, attempts);
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
                Ok(Err(e)) => {
                    last_error = Some(e);
                }
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
            reqwest::StatusCode::IM_A_TEAPOT => return Err(anyhow!("分片尚未就绪 (418)")),
            reqwest::StatusCode::NOT_FOUND => return Err(anyhow!("404_NOT_FOUND")),
            reqwest::StatusCode::FORBIDDEN => return Err(anyhow!("403_FORBIDDEN")),
            reqwest::StatusCode::TOO_MANY_REQUESTS => return Err(anyhow!("请求过于频繁 (429)")),
            status => return Err(anyhow!("下载分片失败: {}", status.as_u16())),
        }

        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if content_type.to_lowercase().contains("text/html") {
            return Err(anyhow!("分片返回了 HTML 而非视频数据"));
        }

        let expected_len = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<usize>().ok());

        let bytes = response.bytes().await?;
        let actual_len = bytes.len();

        if actual_len < MIN_SEGMENT_SIZE {
            if actual_len == 0 {
                return Err(anyhow!("分片下载数据为空"));
            }
            return Err(anyhow!("分片数据过小: {} 字节", actual_len));
        }

        if let Some(expected) = expected_len {
            if actual_len != expected {
                return Err(anyhow!("分片数据不完整: 期望 {} 字节，实际 {} 字节", expected, actual_len));
            }
        }

        Ok(bytes.to_vec())
    }

    async fn convert_ts_to_mp4(&self, ts_path: &Path, mp4_path: &Path) -> Result<()> {
        use std::process::Command;

        info!("[{}] 正在使用 FFmpeg 修正时间戳并无损重整为 MP4...", self.username);

        let mut std_cmd = Command::new("ffmpeg");
        std_cmd
            .arg("-fflags").arg("+genpts+igndts")
            .arg("-f").arg("mp4") 
            .arg("-i").arg(ts_path)
            .arg("-c:a").arg("copy")    
            .arg("-c:v").arg("copy")    
            .arg("-movflags").arg("+faststart+frag_keyframe+empty_moov") 
            .arg("-y")
            .arg(mp4_path);

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
            error!("[{}] FFmpeg 转换详细错误日志: {}", self.username, stderr);
            return Err(anyhow!("视频结构重整失败，请查看上方详细日志。"));
        }

        info!("[{}] MP4 转换完成，已保存到: {:?}", self.username, mp4_path);
        Ok(())
    }

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
