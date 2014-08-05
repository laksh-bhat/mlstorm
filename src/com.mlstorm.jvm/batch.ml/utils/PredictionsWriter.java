package utils;

import dataobject.label.Label;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

public class PredictionsWriter {

    private Writer _writer;

    public PredictionsWriter(String prediction_file) throws IOException {
        this._writer = new BufferedWriter(new FileWriter(prediction_file));
    }

    public void close() throws IOException {
        this._writer.close();
    }

    public void writePrediction(Label label) throws IOException {
        this._writer.write(label.toString());
        this._writer.write("\n");
    }

}
