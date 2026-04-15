
package org.Amkor.qovor.impl;


import org.apache.commons.net.ftp.*;
import org.apache.commons.net.util.TrustManagerUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FtpUploader implements AutoCloseable {


    public static class FtpConfig {

        public String host;
        public int port = 21;
        public String username;
        public String password;
        public String remoteDir = "/";

        public boolean passiveMode = true;
        public boolean ftpsEnabled = false;
        public boolean ftpsImplicit = false;

        public int connectTimeoutMs = 20000;
        public int dataTimeoutMs = 60000;
        public String controlEncoding = "UTF-8";

        public boolean useEPSVwithIPv4 = true;
        public boolean passiveNatWorkaround = true;
        public int controlKeepAliveSec = 30;
        public int controlKeepAliveReplyMs = 10000;

    }

    private final FtpConfig cfg;
    private FTPClient client;

    public FtpUploader(FtpConfig cfg) { this.cfg = cfg; }

    private synchronized void connectIfNeeded() throws IOException {
        if (client != null && client.isConnected()) return;

        FTPClient c;
        if (cfg.ftpsEnabled) {
            FTPSClient ftps = new FTPSClient(cfg.ftpsImplicit);
            ftps.setTrustManager(TrustManagerUtils.getAcceptAllTrustManager());
            c = ftps;
        } else {
            c = new FTPClient();
        }

        c.setControlEncoding(cfg.controlEncoding);
        c.setConnectTimeout(cfg.connectTimeoutMs);
        c.setDataTimeout(cfg.dataTimeoutMs);
        c.setDefaultTimeout(cfg.connectTimeoutMs);

        c.setUseEPSVwithIPv4(cfg.useEPSVwithIPv4);
        c.setPassiveNatWorkaround(cfg.passiveNatWorkaround);


        c.setControlKeepAliveTimeout(cfg.controlKeepAliveSec);
        c.setControlKeepAliveReplyTimeout(cfg.controlKeepAliveReplyMs);



        c.connect(cfg.host, cfg.port);
        int reply = c.getReplyCode();

        if (!FTPReply.isPositiveCompletion(reply)) {
            safeDisconnect(c);
            throw new IOException("FTP connect failed, reply=" + reply + " - " + c.getReplyString());
        }



        if (!c.login(cfg.username, cfg.password)) {
            safeDisconnect(c);
            throw new IOException("FTP login failed for user " + cfg.username + " - " + c.getReplyString());
        }

        if (cfg.ftpsEnabled) {
            FTPSClient ftps = (FTPSClient) c;
            ftps.execPBSZ(0);
            ftps.execPROT("P");
        }

        if (cfg.passiveMode) c.enterLocalPassiveMode(); else c.enterLocalActiveMode();
        c.setFileType(FTP.BINARY_FILE_TYPE);
        c.setFileTransferMode(FTP.STREAM_TRANSFER_MODE);

        this.client = c;
    }


    public void uploadAtomic(String remoteDir, String fileName, InputStream data) throws IOException {
        connectIfNeeded();
        ensureDir(remoteDir);

        String tmpName = fileName + ".tmp";

        try (var out = client.storeFileStream(tmpName)) {
            if (out == null) {
                throw new IOException("storeFileStream returned null: " + client.getReplyString());
            }
            byte[] buf = new byte[64 * 1024];
            int n;
            while ((n = data.read(buf)) != -1) {
                out.write(buf, 0, n);
            }
            out.flush();
        }

        if (!client.completePendingCommand()) {
            client.deleteFile(tmpName);
            throw new IOException("completePendingCommand failed: " + client.getReplyString());
        }

        try { client.deleteFile(fileName); } catch (Exception ignore) {}

        if (!client.rename(tmpName, fileName)) {
            try { client.deleteFile(tmpName); } catch (Exception ignore) {}
            throw new IOException("FTP rename failed: " + client.getReplyString());
        }
    }

    public void moveOldFilesToSftpPairByNameTime(SftpUploader sftp, String ftpFolder,  String sftpTargetDir, int ageHours, boolean deleteAfterMove,
                    String nameTimeRegex, String nameTimeFormat, String zoneID, String assumeHHmmss, String yearHint, String ftpBackupDir) throws IOException{
        connectIfNeeded();

        String ftpDir = normalizePath(ftpFolder);
        String ftpBkDir = normalizePath(ftpBackupDir);

        if (!client.changeWorkingDirectory(ftpDir)) {
            System.out.println("[FTP] Skip (cannot CWD): " + ftpDir + "  Reply=" + client.getReplyString());
            return;
        }


        FTPFile[] files = client.listFiles();
        if (files == null || files.length == 0) return;

        ZoneId zone = (zoneID == null || zoneID.isBlank()) ? ZoneId.systemDefault() : ZoneId.of(zoneID);

        Pattern pat = Pattern.compile(nameTimeRegex);



        try { sftp.ensureDir(sftpTargetDir); }
        catch (Exception e) { throw new IOException("Cannot ensure SFTP dir: " + sftpTargetDir, e); }

        mkdirs(ftpBkDir);

        long nowMs = System.currentTimeMillis();

        long thresholdMs = java.time.Duration.ofHours(ageHours).toMillis();


        for (FTPFile f : files) {
            if (!f.isFile()) continue;

            String name = f.getName();
            Long tsMs = extractEpochMsFromName(name, pat, nameTimeFormat, zone, assumeHHmmss, yearHint);
            if (tsMs == null) {
                System.out.println("[MOVE] Skip (cannot parse time from name): " + name);
                continue;
            }

            long ageMs = nowMs - tsMs;
            if (ageMs < thresholdMs) continue;


            String srcPath = ftpDir + "/" + name;
            String dstPath = ftpBkDir + "/" + name;

            System.out.println("[MOVE] " + ftpDir + "/" + name + " age=" + (ageMs / 3600000L) + "h -> " + sftpTargetDir + "/" + name);

            // 1) Download từ FTP
            ByteArrayOutputStream baos = new ByteArrayOutputStream(
                    (int) Math.min(Math.max(f.getSize(), 0), Integer.MAX_VALUE)
            );

            boolean ok = client.retrieveFile(srcPath, baos);
            if (!ok) {
                System.err.println("[MOVE] retrieveFile FAILED: " + client.getReplyString());
                continue;
            }
            byte[] payload = baos.toByteArray();

            // 2) Upload sang SFTP
            boolean sftpOK;
            try (java.io.InputStream in = new java.io.ByteArrayInputStream(payload)) {
                sftp.uploadAtomic(sftpTargetDir, name, in);
                sftpOK = true;
            } catch (Exception ex) {
                System.err.println("[MOVE] SFTP upload FAILED for " + name + " : " + ex);
                continue;
            }

            //3) backup trên ftp
            boolean ftpBackupOk = false;
            try (InputStream in2 = new ByteArrayInputStream(payload)) {
                if (client.storeFile(dstPath, in2)) {
                    ftpBackupOk = true;
                    System.out.println("[MOVE] FTP stored backup OK: " + dstPath);
                } else {
                    System.err.println("[MOVE] FTP store backup FAILED: " + client.getReplyString());
                }
            } catch (Exception ex) {
                System.err.println("[MOVE] FTP store backup error: " + ex);
            }



            // 4) Xóa file gốc trên FTP nếu cả SFTP & FTP backup đều OK
            if (deleteAfterMove && ftpBackupOk) {
                try {
                    if (!client.deleteFile(srcPath)) {
                        System.err.println("[MOVE] FTP delete original FAILED: " + client.getReplyString());
                    } else {
                        System.out.println("[MOVE] Deleted original on FTP: " + srcPath);
                    }
                } catch (Exception ex) {
                    System.err.println("[MOVE] FTP delete error: " + ex);
                }
            } else if (deleteAfterMove && !ftpBackupOk) {
                System.err.println("[MOVE] WARNING: backup not OK -> original kept: " + srcPath);
            }
        }
    }



    private static Long extractEpochMsFromName(String name,
                                               Pattern pat,
                                               String format,
                                               ZoneId zone,
                                               String assumeHHmmss,
                                               String yearHint) {
        Matcher m = pat.matcher(name);
        if (!m.find()) return null;
        String token = m.group(1);

        try {
            String fmt = format.trim();


            switch (fmt) {
                // Trường hợp 1: yyyyMMddHHmmss (đủ năm-ngày-giờ)
                case "yyyyMMddHHmmss" -> {
                    DateTimeFormatter df = DateTimeFormatter.ofPattern(fmt);
                    LocalDateTime ldt = LocalDateTime.parse(token, df);
                    return ldt.atZone(zone).toInstant().toEpochMilli();
                }

                // Trường hợp 2: yyyyMMdd (chỉ ngày) -> gắn assumeHHmmss (vd: 060000)
                case "yyyyMMdd" -> {
                    DateTimeFormatter df = DateTimeFormatter.ofPattern(fmt);
                    LocalDate date = LocalDate.parse(token, df);
                    LocalTime time = (assumeHHmmss != null && assumeHHmmss.matches("\\d{6}"))
                            ? LocalTime.of(
                            Integer.parseInt(assumeHHmmss.substring(0, 2)),
                            Integer.parseInt(assumeHHmmss.substring(2, 4)),
                            Integer.parseInt(assumeHHmmss.substring(4, 6)))
                            : LocalTime.MIDNIGHT;
                    LocalDateTime ldt = LocalDateTime.of(date, time);
                    return ldt.atZone(zone).toInstant().toEpochMilli();
                }

                // Trường hợp 3: MMddHHmmss (không có năm) -> thêm yearHint/CURRENT
                case "MMddHHmmss" -> {
                    int year;
                    if (yearHint != null && yearHint.matches("\\d{4}")) {
                        year = Integer.parseInt(yearHint);
                    } else {
                        year = LocalDate.now(zone).getYear();
                    }
                    // Parse thủ công theo vị trí
                    int MM = Integer.parseInt(token.substring(0, 2));
                    int dd = Integer.parseInt(token.substring(2, 4));
                    int HH = Integer.parseInt(token.substring(4, 6));
                    int mm = Integer.parseInt(token.substring(6, 8));
                    int ss = Integer.parseInt(token.substring(8, 10));
                    LocalDateTime ldt = LocalDateTime.of(year, MM, dd, HH, mm, ss);
                    return ldt.atZone(zone).toInstant().toEpochMilli();
                }
            }

            // Fallback thông minh theo độ dài token
            if (token.length() == 14) {
                DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
                LocalDateTime ldt = LocalDateTime.parse(token, df);
                return ldt.atZone(zone).toInstant().toEpochMilli();
            }
            if (token.length() == 8) {
                DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyyMMdd");
                LocalDate date = LocalDate.parse(token, df);
                LocalDateTime ldt = LocalDateTime.of(date,
                        (assumeHHmmss != null && assumeHHmmss.matches("\\d{6}"))
                                ? LocalTime.of(
                                Integer.parseInt(assumeHHmmss.substring(0,2)),
                                Integer.parseInt(assumeHHmmss.substring(2,4)),
                                Integer.parseInt(assumeHHmmss.substring(4,6)))
                                : LocalTime.MIDNIGHT);
                return ldt.atZone(zone).toInstant().toEpochMilli();
            }
            if (token.length() == 10) {
                int year = (yearHint != null && yearHint.matches("\\d{4}"))
                        ? Integer.parseInt(yearHint)
                        : LocalDate.now(zone).getYear();
                int MM = Integer.parseInt(token.substring(0,2));
                int dd = Integer.parseInt(token.substring(2,4));
                int HH = Integer.parseInt(token.substring(4,6));
                int mm = Integer.parseInt(token.substring(6,8));
                int ss = Integer.parseInt(token.substring(8,10));
                LocalDateTime ldt = LocalDateTime.of(year, MM, dd, HH, mm, ss);
                return ldt.atZone(zone).toInstant().toEpochMilli();
            }

            return null;
        } catch (Exception e) {
            return null;
        }
    }




    private static String normalizePath(String p) {
        if (p == null || p.isBlank()) return "/";
        String r = p.replace('\\', '/').trim();
        if (r.length() > 1 && r.endsWith("/")) r = r.substring(0, r.length() - 1);
        return r.startsWith("/") ? r : "/" + r;
    }

    public void ensureDir(String remoteDir) throws IOException {
        connectIfNeeded();
        if (!client.changeWorkingDirectory(remoteDir)) {
            mkdirs(remoteDir);
            if (!client.changeWorkingDirectory(remoteDir)) {
                throw new IOException("Cannot change to remoteDir " + remoteDir);
            }
        }
    }

    private void mkdirs(String path) throws IOException {
        connectIfNeeded();
        String[] parts = path.split("/");
        String current = "";
        for (String p : parts) {
            if (p == null || p.isBlank()) continue;
            current += "/" + p;
            if (!client.changeWorkingDirectory(current)) {
                if (!client.makeDirectory(current)) {
                    client.setControlEncoding(StandardCharsets.UTF_8.name());
                    if (!client.makeDirectory(current)) {
                        throw new IOException("Failed to mkdir " + current + ": " + client.getReplyString());
                    }
                }
            }
        }
    }


    private static void safeDisconnect(FTPClient c) {
        try { c.disconnect(); } catch (Exception ignore) {}
    }

    @Override
    public void close() {
        if (client != null && client.isConnected()) {
            try { client.logout(); } catch (Exception ignore) {}
            try { client.disconnect(); } catch (Exception ignore) {}
        }
    }
}
