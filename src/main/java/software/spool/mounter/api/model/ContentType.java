package software.spool.mounter.api.model;

import java.nio.charset.StandardCharsets;

public enum ContentType {
    PDF, XML, HTML, UNKNOWN;

    public static ContentType detect(byte[] bytes) {
        if (bytes == null || bytes.length < 4) return UNKNOWN;

        String header = new String(bytes, 0, Math.min(bytes.length, 16), StandardCharsets.UTF_8)
                .trim()
                .toLowerCase();

        if (header.startsWith("%pdf"))           return PDF;
        if (header.startsWith("<?xml"))          return XML;
        if (header.startsWith("<html"))          return HTML;
        if (header.startsWith("<!doctype html")) return HTML;

        return UNKNOWN;
    }
}