package software.spool.mounter.api.port;

import software.spool.mounter.api.model.ContentType;

@FunctionalInterface
public interface ExtensionResolver {
    String resolve(byte[] content);

    static ExtensionResolver fixed(String extension) {
        return content -> extension;
    }

    static ExtensionResolver auto() {
        return content -> switch (ContentType.detect(content)) {
            case PDF  -> "pdf";
            case XML  -> "xml";
            case HTML -> "html";
            default   -> "bin";
        };
    }
}