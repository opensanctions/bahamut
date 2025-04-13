package org.opensanctions.zahir;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import tech.followthemoney.model.Model;
import tech.followthemoney.model.Schema;
import tech.followthemoney.statement.Statement;

public class DataFixtures {
    
    public static List<Statement> getFixtureStatements(String dataset) {
        Model model = ModelFixtures.getModel();
        List<Statement> statements = new ArrayList<>();
        String fixturePath = "/fixtures/%s.csv".formatted(dataset);
        InputStream inputStream = DataFixtures.class.getResourceAsStream(fixturePath);
        if (inputStream == null) {
            throw new RuntimeException("Fixture not found: " + DataFixtures.class.getResourceAsStream(fixturePath));
            
        }
        CSVFormat format = CSVFormat.DEFAULT.builder().setHeader().get();
        try (InputStreamReader reader = new InputStreamReader(inputStream);
            CSVParser parser = format.parse(reader)) {
            for (CSVRecord record : parser) {
                Schema schema = model.getSchema(record.get("schema"));
                Statement statement = new Statement(
                    record.get("id"),
                    record.get("canonical_id"),
                    record.get("entity_id"),
                    schema,
                    record.get("prop"),
                    record.get("dataset"),
                    record.get("value"),
                    record.get("lang"),
                    record.get("original_value"),
                    record.get("external").equals("t"),
                    0,
                    0
                );
                statements.add(statement);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read fixture: " + dataset, e);
        }
        return statements;
    }
}
