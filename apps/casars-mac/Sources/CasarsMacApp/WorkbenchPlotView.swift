import CasarsMacCore
import SwiftUI

struct PlotSamplesPanel: View {
    @ObservedObject var store: WorkbenchStore

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 18) {
                PanelHeader(
                    title: "Plot Samples",
                    subtitle: "Reusable workbench plot widget samples"
                )

                ForEach(store.state.plotDocuments) { plot in
                    PlotSampleCard(store: store, plot: plot)
                }
            }
            .padding(20)
        }
        .accessibilityIdentifier("panel.plotSamples")
    }
}

private struct PlotSampleCard: View {
    @ObservedObject var store: WorkbenchStore
    let plot: WorkbenchPlotDocument

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(alignment: .firstTextBaseline) {
                VStack(alignment: .leading, spacing: 2) {
                    Text(plot.title)
                        .workbenchFont(.headline)
                    Text(plot.subtitle)
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                Text("\(pointCount) pts")
                    .workbenchFont(.caption, design: .monospaced)
                    .foregroundStyle(.secondary)
                    .accessibilityIdentifier("plotSamples.\(plot.id).pointCount")
            }

            WorkbenchPlotView(plot: plot)
                .frame(height: plot.layers.contains { $0.kind == .raster } ? 360 : 320)
                .overlay(
                    RoundedRectangle(cornerRadius: 6)
                        .stroke(Color.secondary.opacity(0.22))
                )
                .accessibilityIdentifier("plotSamples.\(plot.id).canvas")

            controls
        }
        .padding()
        .background(.regularMaterial)
        .clipShape(RoundedRectangle(cornerRadius: 8))
        .accessibilityIdentifier("plotSamples.\(plot.id)")
    }

    private var controls: some View {
        VStack(alignment: .leading, spacing: 10) {
            if let layer = plot.layers.first(where: { $0.kind == .scatter }) {
                SliderRow(
                    title: "Symbol",
                    value: layer.style.symbolSize,
                    range: 1...12,
                    format: "%.1f"
                ) { value in
                    store.applyWorkbenchPlotEdit(
                        plotID: plot.id,
                        action: .setLayerSymbolSize(layerID: layer.id, size: value)
                    )
                }
                .accessibilityIdentifier("plotSamples.\(plot.id).symbolSize")
            }

            if let layer = plot.layers.first(where: { $0.kind == .line }) {
                SliderRow(
                    title: "Line",
                    value: layer.style.lineWidth,
                    range: 0.5...8,
                    format: "%.1f"
                ) { value in
                    store.applyWorkbenchPlotEdit(
                        plotID: plot.id,
                        action: .setLayerLineWidth(layerID: layer.id, width: value)
                    )
                }
                .accessibilityIdentifier("plotSamples.\(plot.id).lineWidth")
            }

            if let layer = plot.layers.first {
                Toggle("Layer visible", isOn: Binding(
                    get: { layer.style.visible },
                    set: { visible in
                        store.applyWorkbenchPlotEdit(
                            plotID: plot.id,
                            action: .setLayerVisibility(layerID: layer.id, visible: visible)
                        )
                    }
                ))
                .workbenchFont(.caption)
                .accessibilityIdentifier("plotSamples.\(plot.id).layerVisible")
            }

            if let rasterLayer = plot.layers.first(where: { $0.raster != nil }) {
                HStack(spacing: 12) {
                    Picker("Stretch", selection: Binding(
                        get: { rasterLayer.raster?.stretch ?? .linear },
                        set: { stretch in
                            store.applyWorkbenchPlotEdit(
                                plotID: plot.id,
                                action: .setRasterStretch(layerID: rasterLayer.id, stretch: stretch)
                            )
                        }
                    )) {
                        ForEach(WorkbenchPlotImageStretch.allCases) { stretch in
                            Text(stretch.rawValue).tag(stretch)
                        }
                    }
                    .pickerStyle(.menu)
                    .frame(width: 170)
                    .accessibilityIdentifier("plotSamples.\(plot.id).stretch")

                    Picker("Color map", selection: Binding(
                        get: { rasterLayer.raster?.colorMap ?? .viridis },
                        set: { colorMap in
                            store.applyWorkbenchPlotEdit(
                                plotID: plot.id,
                                action: .setRasterColorMap(layerID: rasterLayer.id, colorMap: colorMap)
                            )
                        }
                    )) {
                        ForEach(WorkbenchPlotColorMap.allCases) { colorMap in
                            Text(colorMap.rawValue).tag(colorMap)
                        }
                    }
                    .pickerStyle(.menu)
                    .frame(width: 170)
                    .accessibilityIdentifier("plotSamples.\(plot.id).colorMap")
                }
            }
        }
    }

    private var pointCount: Int {
        plot.layers.reduce(0) { total, layer in total + layer.points.count }
    }
}

private struct SliderRow: View {
    var title: String
    var value: Double
    var range: ClosedRange<Double>
    var format: String
    var update: (Double) -> Void

    var body: some View {
        HStack(spacing: 10) {
            Text(title)
                .frame(width: 58, alignment: .leading)
                .workbenchFont(.caption)
            Slider(
                value: Binding(
                    get: { value },
                    set: update
                ),
                in: range
            )
            Text(String(format: format, value))
                .frame(width: 42, alignment: .trailing)
                .workbenchFont(.caption, design: .monospaced)
                .foregroundStyle(.secondary)
        }
    }
}

struct WorkbenchPlotView: View {
    let plot: WorkbenchPlotDocument

    var body: some View {
        Canvas { context, size in
            let plotRect = plotRect(for: size)
            drawBackground(in: &context, size: size, plotRect: plotRect)
            drawRasterLayers(in: &context, plotRect: plotRect)
            drawVectorLayers(in: &context, plotRect: plotRect)
            drawAnnotations(in: &context, plotRect: plotRect)
            drawAxes(in: &context, size: size, plotRect: plotRect)
        }
        .background(Color(nsColor: .textBackgroundColor))
    }

    private var xAxis: WorkbenchPlotAxis? {
        plot.axes.first
    }

    private var yAxis: WorkbenchPlotAxis? {
        plot.axes.dropFirst().first
    }

    private func plotRect(for size: CGSize) -> CGRect {
        let left = 64.0
        let top = 26.0
        let right = 26.0
        let bottom = 56.0
        return CGRect(
            x: left,
            y: top,
            width: max(20, size.width - left - right),
            height: max(20, size.height - top - bottom)
        )
    }

    private func drawBackground(in context: inout GraphicsContext, size: CGSize, plotRect: CGRect) {
        context.fill(Path(CGRect(origin: .zero, size: size)), with: .color(Color(nsColor: .textBackgroundColor)))
        context.fill(Path(plotRect), with: .color(Color(nsColor: .controlBackgroundColor)))

        let gridColor = Color.secondary.opacity(0.16)
        for fraction in stride(from: 0.0, through: 1.0, by: 0.2) {
            let x = plotRect.minX + plotRect.width * fraction
            var vertical = Path()
            vertical.move(to: CGPoint(x: x, y: plotRect.minY))
            vertical.addLine(to: CGPoint(x: x, y: plotRect.maxY))
            context.stroke(vertical, with: .color(gridColor), lineWidth: 1)

            let y = plotRect.minY + plotRect.height * fraction
            var horizontal = Path()
            horizontal.move(to: CGPoint(x: plotRect.minX, y: y))
            horizontal.addLine(to: CGPoint(x: plotRect.maxX, y: y))
            context.stroke(horizontal, with: .color(gridColor), lineWidth: 1)
        }
    }

    private func drawRasterLayers(in context: inout GraphicsContext, plotRect: CGRect) {
        for layer in plot.layers where layer.kind == .raster && layer.style.visible {
            guard let raster = layer.raster, raster.width > 0, raster.height > 0 else { continue }
            let cellWidth = plotRect.width / Double(raster.width)
            let cellHeight = plotRect.height / Double(raster.height)
            for y in 0..<raster.height {
                for x in 0..<raster.width {
                    let index = y * raster.width + x
                    guard index < raster.values.count else { continue }
                    let value = normalizedRasterValue(raster.values[index], raster: raster)
                    let color = rasterColor(value, colorMap: raster.colorMap).opacity(layer.style.opacity)
                    let cell = CGRect(
                        x: plotRect.minX + Double(x) * cellWidth,
                        y: plotRect.maxY - Double(y + 1) * cellHeight,
                        width: ceil(cellWidth) + 0.5,
                        height: ceil(cellHeight) + 0.5
                    )
                    context.fill(Path(cell), with: .color(color))
                }
            }
        }
    }

    private func drawVectorLayers(in context: inout GraphicsContext, plotRect: CGRect) {
        for layer in plot.layers where layer.style.visible {
            switch layer.kind {
            case .scatter:
                let color = color(hex: layer.style.colorHex).opacity(layer.style.opacity)
                for point in layer.points {
                    guard let position = screenPoint(point, plotRect: plotRect) else { continue }
                    let radius = max(0.5, layer.style.symbolSize / 2)
                    context.fill(
                        Path(ellipseIn: CGRect(
                            x: position.x - radius,
                            y: position.y - radius,
                            width: radius * 2,
                            height: radius * 2
                        )),
                        with: .color(color)
                    )
                }
            case .line:
                let points = layer.points.compactMap { screenPoint($0, plotRect: plotRect) }
                guard points.count > 1 else { continue }
                var path = Path()
                path.move(to: points[0])
                for point in points.dropFirst() {
                    path.addLine(to: point)
                }
                context.stroke(
                    path,
                    with: .color(color(hex: layer.style.colorHex).opacity(layer.style.opacity)),
                    lineWidth: layer.style.lineWidth
                )
            case .raster:
                continue
            }
        }
    }

    private func drawAnnotations(in context: inout GraphicsContext, plotRect: CGRect) {
        for annotation in plot.annotations {
            let point = WorkbenchPlotPoint(x: annotation.x, y: annotation.y)
            guard let position = screenPoint(point, plotRect: plotRect) else { continue }
            let label = Text(annotation.text).font(.caption)
            context.draw(label, at: CGPoint(x: position.x + 6, y: position.y - 8), anchor: .leading)
            context.fill(
                Path(ellipseIn: CGRect(x: position.x - 2, y: position.y - 2, width: 4, height: 4)),
                with: .color(.primary)
            )
        }
    }

    private func drawAxes(in context: inout GraphicsContext, size: CGSize, plotRect: CGRect) {
        let frame = Path(plotRect)
        context.stroke(frame, with: .color(Color.secondary.opacity(0.45)), lineWidth: 1)

        if let xAxis {
            drawTicks(axis: xAxis, horizontal: true, in: &context, plotRect: plotRect)
            if xAxis.labelsVisible {
                context.draw(
                    Text(axisLabel(xAxis)).font(.caption),
                    at: CGPoint(x: plotRect.midX, y: size.height - 20),
                    anchor: .center
                )
            }
        }
        if let yAxis {
            drawTicks(axis: yAxis, horizontal: false, in: &context, plotRect: plotRect)
            if yAxis.labelsVisible {
                context.draw(
                    Text(axisLabel(yAxis)).font(.caption),
                    at: CGPoint(x: 12, y: plotRect.midY),
                    anchor: .leading
                )
            }
        }
    }

    private func drawTicks(
        axis: WorkbenchPlotAxis,
        horizontal: Bool,
        in context: inout GraphicsContext,
        plotRect: CGRect
    ) {
        for fraction in stride(from: 0.0, through: 1.0, by: 0.25) {
            let value = axis.range.lower + axis.range.span * fraction
            let text = Text(shortNumber(value)).font(.caption2)
            if horizontal {
                let x = plotRect.minX + plotRect.width * fraction
                context.draw(text, at: CGPoint(x: x, y: plotRect.maxY + 16), anchor: .center)
            } else {
                let y = plotRect.maxY - plotRect.height * fraction
                context.draw(text, at: CGPoint(x: plotRect.minX - 8, y: y), anchor: .trailing)
            }
        }
    }

    private func screenPoint(_ point: WorkbenchPlotPoint, plotRect: CGRect) -> CGPoint? {
        guard let xAxis, let yAxis, xAxis.range.span != 0, yAxis.range.span != 0 else {
            return nil
        }
        let x = (point.x - xAxis.range.lower) / xAxis.range.span
        let y = (point.y - yAxis.range.lower) / yAxis.range.span
        guard x.isFinite, y.isFinite else { return nil }
        return CGPoint(
            x: plotRect.minX + plotRect.width * x,
            y: plotRect.maxY - plotRect.height * y
        )
    }

    private func axisLabel(_ axis: WorkbenchPlotAxis) -> String {
        axis.unit.isEmpty ? axis.label : "\(axis.label) (\(axis.unit))"
    }

    private func shortNumber(_ value: Double) -> String {
        if abs(value) >= 100 {
            return String(format: "%.0f", value)
        }
        if abs(value) >= 10 {
            return String(format: "%.1f", value)
        }
        return String(format: "%.2f", value)
    }

    private func normalizedRasterValue(_ value: Double, raster: WorkbenchPlotRaster) -> Double {
        let span = raster.valueRange.span
        guard span > 0 else { return 0 }
        let clamped = min(1, max(0, (value - raster.valueRange.lower) / span))
        switch raster.stretch {
        case .linear:
            return clamped
        case .squareRoot:
            return sqrt(clamped)
        case .logarithmic:
            return log10(1 + 99 * clamped) / 2
        case .percentile:
            return min(1, max(0, (clamped - 0.05) / 0.9))
        }
    }

    private func rasterColor(_ value: Double, colorMap: WorkbenchPlotColorMap) -> Color {
        let t = min(1, max(0, value))
        switch colorMap {
        case .viridis:
            return Color(red: 0.16 + 0.68 * t, green: 0.10 + 0.78 * sqrt(t), blue: 0.35 + 0.22 * (1 - t))
        case .magma:
            return Color(red: 0.05 + 0.95 * t, green: 0.02 + 0.42 * pow(t, 1.6), blue: 0.12 + 0.48 * (1 - t))
        case .grayscale:
            return Color(white: t)
        case .coolWarm:
            return Color(red: t, green: 0.24 + 0.45 * (1 - abs(t - 0.5) * 2), blue: 1 - t)
        }
    }

    private func color(hex: String) -> Color {
        let trimmed = hex.trimmingCharacters(in: CharacterSet(charactersIn: "#"))
        guard trimmed.count == 6, let value = UInt64(trimmed, radix: 16) else {
            return .accentColor
        }
        return Color(
            red: Double((value >> 16) & 0xff) / 255,
            green: Double((value >> 8) & 0xff) / 255,
            blue: Double(value & 0xff) / 255
        )
    }
}
