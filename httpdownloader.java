
package org.Amkor.qovor.impl;

import java.io.InputStream;
import java.net.URI;
import java.net.http.*;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import java.net.CookieManager;
import java.net.CookiePolicy;
import java.net.http.HttpClient;

public class HttpDownloader {

    private final HttpClient client;

    public HttpDownloader() {
        CookieManager cm = new CookieManager(null, CookiePolicy.ACCEPT_ALL);
        this.client = HttpClient.newBuilder()
                .cookieHandler(cm)
                .followRedirects(HttpClient.Redirect.ALWAYS)
                .build();
    }

    public Download open(String url) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .GET()
                .header("Accept",
                        "application/vnd.ms-excel, " +
                                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, " +
                                "application/octet-stream;q=0.9, */*;q=0.1")
                .header("User-Agent", "Mozilla/5.0")
                .build();

        HttpResponse<InputStream> resp = client.send(req, HttpResponse.BodyHandlers.ofInputStream());

        String ct = resp.headers().firstValue("Content-Type").orElse("").toLowerCase();
        String cd = resp.headers().firstValue("Content-Disposition").orElse("").toLowerCase();

        boolean seemsExcel =
                cd.contains(".xls") || cd.contains(".xlsx") ||
                        cd.contains("attachment") ||
                        ct.contains("application/vnd.ms-excel") ||
                        ct.contains("application/vnd.openxmlformats-officedocument") ||
                        ct.contains("application/octet-stream");

        if (!seemsExcel) {
            throw new IllegalStateException("Server returned non-file content (Content-Type=" + ct + ").");
        }


        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            throw new IllegalStateException("HTTP " + resp.statusCode() + " for " + url);
        }

        String contentType = resp.headers().firstValue("Content-Type").orElse("").toLowerCase();
        String contentDisp = resp.headers().firstValue("Content-Disposition").orElse("");

        boolean seemsBinary = contentType.contains("application/vnd.ms-excel")
                || contentType.contains("application/vnd.openxmlformats-officedocument")
                || contentType.contains("application/octet-stream")
                || contentDisp.toLowerCase().contains("attachment");

        if (!seemsBinary) {
            throw new IllegalStateException(
                    "Server returned non-file content (Content-Type=" + contentType + "). "
            );
        }

        String fileName = extractFileName(resp, url);
        long len = resp.headers().firstValueAsLong("Content-Length").orElse(-1L);
        return new Download(resp.body(), fileName, len);
    }

    public static final class Download implements AutoCloseable {
        public final InputStream stream;
        public final String originalFileName;
        public final long contentLength;

        public Download(InputStream stream, String originalFileName, long contentLength) {
            this.stream = stream;
            this.originalFileName = originalFileName;
            this.contentLength = contentLength;
        }
        @Override public void close() throws Exception { stream.close(); }
    }

    private static String extractFileName(HttpResponse<?> resp, String url) {
        Optional<String> cdOpt = resp.headers().firstValue("Content-Disposition");
        if (cdOpt.isPresent()) {
            String cd = cdOpt.get();
            String name = parseContentDisposition(cd);
            if (name != null && !name.isBlank()) return name;
        }
        String path = URI.create(url).getPath();
        String fallback = (path == null || path.isBlank())
                ? "download.bin"
                : path.substring(path.lastIndexOf('/') + 1);
        return fallback.isBlank() ? "download.bin" : fallback;
    }

    private static String parseContentDisposition(String cd) {
        Pattern p = Pattern.compile("filename\\*?=([^;]+)", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(cd);
        if (m.find()) {
            String v = m.group(1).trim();
            v = v.replaceAll("^UTF-8''", "");
            v = v.replaceAll("^\"|\"$", "");
            return v;
        }
        return null;
    }
}

