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
const MAX_CONSECUTIVE_FAILED_SEGMENTS: u32 = 3; // 新增：主动止损阈值

#[derive(Debug, PartialEq)]
enum RoundOutcome {
    DownloadedContent,
    NoNewSegments,
    PendingRetry,
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
                Ok(_) => true,
                Err(_) => false,
            }
        } else { false }
    }

    async fn interruptible_sleep(&mut self, duration: Duration) -> bool {
        if let Some(ref mut shutdown_rx) = self.shutdown_rx {
            let mut shutdown_rx = shutdown_rx.resubscribe();
            tokio::select! {
                _ = tokio::time::sleep(duration) => self.check_shutdown_signal(),
                _ = shutdown_rx.recv() => true,
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
    where F: Fn(&str) -> String,
    {
        let mut part_index = 1;
        let max_duration = Duration::from_secs(60 * 60);

        'record_session: loop {
            let final_mp4_path = if part_index == 1 {
                output_path.with_extension("mp4")
            } else {
                let file_stem = output_path.file_stem().unwrap_or_default().to_string_lossy();
                output_path.with_file_name(format!("{}_part{}.mp4", file_stem, part_index))
            };

            let temp_raw_path = output_path.with_extension(format!("tmp{}.mp4", part_index));
            let file = File::create(&temp_raw_path).await?;
            let mut output_file = BufWriter::with_capacity(256 * 1024, file);
            let mut has_downloaded_content = false;
            let mut received_shutdown = false;
            let start_time = Instant::now();

            let mut shutdown_rx = self.shutdown_rx.as_mut().map(|rx| rx.resubscribe());

            let download_result = 'poll_loop: loop {
                if start_time.elapsed() >= max_duration { break 'poll_loop Ok(()); }

                let round_result = if let Some(ref mut rx) = shutdown_rx {
                    tokio::select! {
                        r = self.download_playlist_segments(playlist_url, &mut output_file, m3u_processor) => r,
                        _ = rx.recv() => { received_shutdown = true; break 'poll_loop Ok(()); }
                    }
                } else {
                    self.download_playlist_segments(playlist_url, &mut output_file, m3u_processor).await
                };

                match round_result {
                    Ok((RoundOutcome::DownloadedContent, _)) => { has_downloaded_content = true; }
                    Ok((_, _)) => {}
                    Err(e) => {
                        error!("[{}] 网络错误: {}，触发强制切断重连...", self.username, e);
                        break 'poll_loop Ok(());
                    }
                }
            };

            drop(output_file);
            if has_downloaded_content {
                if let Ok(()) = self.convert_raw_to_mp4(&temp_raw_path, &final_mp4_path).await {
                    let _ = tokio::fs::remove_file(&temp_raw_path).await;
                }
            } else { let _ = tokio::fs::remove_file(&temp_raw_path).await; }

            if received_shutdown { return Ok(()); }
            part_index += 1;
            self.init_segment_downloaded = false;
        }
    }

    async fn download_playlist_segments<F>(
        &mut self,
        playlist_url: &str,
        output_file: &mut BufWriter<File>,
        m3u_processor: Option<&F>,
    ) -> Result<(RoundOutcome, u64)>
    where F: Fn(&str) -> String,
    {
        let response = self.client.get(playlist_url).send().await?;
        let mut content = response.text().await?;
        if let Some(p) = m3u_processor { content = p(&content); }

        // 下载逻辑简化，加入失败计数
        let mut consecutive_failed = 0;
        // ... (省略部分重复的解析逻辑，请保持你原有的下载循环逻辑) ...
        // 关键点：在下载 for 循环中加入：
        // if err { consecutive_failed += 1; if consecutive_failed >= MAX_CONSECUTIVE_FAILED_SEGMENTS { return Err(anyhow!("网络崩溃")); } }
        
        Ok((RoundOutcome::DownloadedContent, 6)) // 示例返回
    }

    async fn convert_raw_to_mp4(&self, input_path: &Path, mp4_path: &Path) -> Result<()> {
        use std::process::Command;
        let mut std_cmd = Command::new("ffmpeg");
        std_cmd.arg("-fflags").arg("+genpts+igndts")
               .arg("-f").arg("mp4")
               .arg("-i").arg(input_path)
               .arg("-c").arg("copy")
               .arg("-movflags").arg("+faststart+frag_keyframe+empty_moov")
               .arg("-y").arg(mp4_path);
        
        let output = tokio::process::Command::from(std_cmd).output().await?;
        if !output.status.success() { return Err(anyhow!("FFmpeg 重整失败")); }
        Ok(())
    }
}
