using System;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using SharpCompress.Readers;

namespace Starshot.Helpers;

public static class ArchiveDownloader
{
    /// <summary>
    /// 真流式解压：网络流直连 SharpCompress Reader，逐 entry 写到 destDir。不落 zip、不依赖中央目录。
    /// 进度按网络已读字节 / Content-Length 计算（流式下载解压一体）。
    /// </summary>
    public static async Task ExtractToDirectoryAsync(
        string zipUrl,
        string destDir,
        IProgress<(int percent, string bytesText)>? progress,
        CancellationToken ct = default,
        bool disableCert = false
    )
    {
        string destFull =
            Path.GetFullPath(destDir)
                .TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
            + Path.DirectorySeparatorChar;

        progress?.Report((0, ""));

        var handler = new HttpClientHandler();
        if (disableCert)
        {
            handler.ServerCertificateCustomValidationCallback = (_, _, _, _) => true;
        }
        using var http = new HttpClient(handler);
        http.DefaultRequestHeaders.UserAgent.ParseAdd("Starshot");
        // ResponseHeadersRead：只读响应头，拿 Content-Length 后直接拿流（不缓冲整个响应体）
        using var resp = await http.GetAsync(zipUrl, HttpCompletionOption.ResponseHeadersRead, ct);
        resp.EnsureSuccessStatusCode();
        long? total = resp.Content.Headers.ContentLength;
        await using var httpStream = await resp.Content.ReadAsStreamAsync(ct);
        // CountingStream 包装统计已读字节，SharpCompress 透过它读网络流
        using var counting = new CountingStream(httpStream);
        using var reader = ReaderFactory.OpenReader(counting);

        var buf = new byte[81920];
        while (reader.MoveToNextEntry())
        {
            ct.ThrowIfCancellationRequested();
            if (reader.Entry.IsDirectory)
                continue;
            string? key = reader.Entry.Key;
            if (string.IsNullOrEmpty(key))
                continue;

            // zip slip 防护：目标必须在 destDir 下
            string dest = Path.GetFullPath(
                Path.Combine(destDir, key.Replace('/', Path.DirectorySeparatorChar))
            );
            if (!dest.StartsWith(destFull, StringComparison.OrdinalIgnoreCase))
                continue;

            Directory.CreateDirectory(Path.GetDirectoryName(dest)!);
            using var entryStream = reader.OpenEntryStream();
            using var fs = File.Create(dest);
            int n;
            while ((n = await entryStream.ReadAsync(buf, 0, buf.Length, ct)) > 0)
            {
                await fs.WriteAsync(buf, 0, n, ct);
                if (total > 0)
                {
                    // 留 100% 给调用方 await 返回（API return 作完成标志），中间只到 99
                    int pct = (int)(counting.BytesRead * 99 / total.Value);
                    progress?.Report(
                        (pct, $"{FormatSize(counting.BytesRead)} / {FormatSize(total.Value)}")
                    );
                }
            }
        }
        // 不在这里报 100%：完成标志是本方法 return（调用方 await 返回），progress 末尾到 99
    }

    private static string FormatSize(long bytes)
    {
        if (bytes < 1024)
            return $"{bytes}B";
        if (bytes < 1024 * 1024)
            return $"{bytes / 1024.0:F1}KB";
        if (bytes < 1024L * 1024 * 1024)
            return $"{bytes / (1024.0 * 1024):F1}MB";
        return $"{bytes / (1024.0 * 1024 * 1024):F1}GB";
    }

    /// <summary>
    /// 只读包装流，统计已读字节总数（用于流式解压进度）。
    /// </summary>
    private sealed class CountingStream : Stream
    {
        private readonly Stream _inner;
        public long BytesRead { get; private set; }

        public CountingStream(Stream inner) => _inner = inner;

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => _inner.Length;
        public override long Position
        {
            get => _inner.Position;
            set => throw new NotSupportedException();
        }

        public override void Flush() { }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int n = _inner.Read(buffer, offset, count);
            BytesRead += n;
            return n;
        }

        public override async Task<int> ReadAsync(
            byte[] buffer,
            int offset,
            int count,
            CancellationToken cancellationToken
        )
        {
            int n = await _inner.ReadAsync(buffer, offset, count, cancellationToken);
            BytesRead += n;
            return n;
        }

        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();
    }
}
