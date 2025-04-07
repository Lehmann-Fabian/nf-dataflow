package nextflow.dataflow.helper;

import groovy.transform.CompileStatic;

import java.awt.*;
import java.util.ArrayList;
import java.util.List;

@CompileStatic
public class DistinctColorGenerator {

    public static List<String> generateDistinctColors(int n) {
        List<String> colors = new ArrayList<>();
        float saturation = 0.7f;
        float brightness = 0.9f;

        for (int i = 0; i < n; i++) {
            float hue = (float) i / n;
            final Color color = Color.getHSBColor( hue, saturation, brightness );
            String hexColor = String.format("#%02x%02x%02x", color.getRed(), color.getGreen(), color.getBlue());
            colors.add( hexColor );
        }

        return colors;
    }

}
