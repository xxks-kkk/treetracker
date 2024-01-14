package org.zhu45.treetracker.relational.statistics;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static java.lang.String.format;

public class CostModelDataRepository
{
    private final ObjectMapper objectMapper = new ObjectMapper();

    public void save(String queryName, CostModelStatisticsData costModelStatisticsData)
    {
        Path path = Paths.get("src", "main", "resources", queryName, costModelStatisticsData.getId() + ".json");
        writeStatistics(path, costModelStatisticsData);
    }

    private void writeStatistics(Path path, CostModelStatisticsData costModelStatisticsData)
    {
        File file = path.toFile();
        file.getParentFile().mkdirs();
        try {
            objectMapper
                    .writerWithDefaultPrettyPrinter()
                    .writeValue(file, costModelStatisticsData);
            try (FileWriter fileWriter = new FileWriter(file, true)) {
                fileWriter.append('\n');
            }
        }
        catch (IOException e) {
            throw new RuntimeException("Could not save cost model", e);
        }
    }

    public Optional<CostModelStatisticsData> load(String queryName, String modelDataId, Class<? extends CostModelStatisticsData> costModelStatisticsClazz)
    {
        Path path = Paths.get("src", "main", "resources", queryName, modelDataId + ".json");
        return readStatistics(path, costModelStatisticsClazz);
    }

    private Optional<CostModelStatisticsData> readStatistics(Path path, Class<? extends CostModelStatisticsData> costModelStatisticsClazz)
    {
        if (!Files.exists(path)) {
            return Optional.empty();
        }
        try {
            return Optional.of(objectMapper.readValue(path.toFile(), costModelStatisticsClazz));
        }
        catch (Exception e) {
            throw new RuntimeException(format("Failed to parse stats from resource [%s]", path), e);
        }
    }
}
