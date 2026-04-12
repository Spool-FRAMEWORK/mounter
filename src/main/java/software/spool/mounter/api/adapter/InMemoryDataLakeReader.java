package software.spool.mounter.api.adapter;

import software.spool.core.model.vo.PartitionKey;
import software.spool.mounter.api.port.DataLakeReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;
import java.util.stream.Stream;

public class InMemoryDataLakeReader<I> implements DataLakeReader<I> {

    private final Function<Path, Stream<I>> fileDeserializer;

    public InMemoryDataLakeReader(Function<Path, Stream<I>> fileDeserializer) {
        this.fileDeserializer = fileDeserializer;
    }

    @Override
    public Stream<I> read(PartitionKey partitionKey) {
        URL resourcesUrl = getClass().getClassLoader().getResource("");
        if (resourcesUrl == null) {
            return Stream.empty();
        }

        try {
            Path root = Paths.get(resourcesUrl.toURI());
            return Files.walk(root)
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString()
                            .contains(partitionKey.value()))
                    .flatMap(fileDeserializer);
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Resources path could not be resolved", e);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to walk resources for partition: " + partitionKey, e);
        }
    }
}