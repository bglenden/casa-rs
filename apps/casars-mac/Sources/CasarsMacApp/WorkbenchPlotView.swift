import CasarsMacCore
import CoreGraphics
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
    @StateObject private var pointRasterSummaryCache = WorkbenchPointRasterCache()
    @State private var controlsExpanded = false
    @State private var plotCanvasSize = CGSize.zero

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
                Text(sampleCountSummary)
                    .workbenchFont(.caption, design: .monospaced)
                    .foregroundStyle(.secondary)
                    .accessibilityIdentifier("plotSamples.\(plot.id).pointCount")
            }

            WorkbenchPlotView(plot: plot)
                .frame(height: plot.allLayers.contains { $0.kind == .raster } ? 360 : (plot.panels.isEmpty ? 320 : 420))
                .background(
                    GeometryReader { proxy in
                        Color.clear.preference(key: PlotCanvasSizePreferenceKey.self, value: proxy.size)
                    }
                )
                .overlay(
                    RoundedRectangle(cornerRadius: 6)
                        .stroke(Color.secondary.opacity(0.22))
                )
                .accessibilityIdentifier("plotSamples.\(plot.id).canvas")
                .onPreferenceChange(PlotCanvasSizePreferenceKey.self) { size in
                    plotCanvasSize = size
                }

            DisclosureGroup("Display controls", isExpanded: $controlsExpanded) {
                controls
                    .padding(.top, 8)
            }
            .workbenchFont(.caption)
        }
        .padding()
        .background(.regularMaterial)
        .clipShape(RoundedRectangle(cornerRadius: 8))
        .accessibilityIdentifier("plotSamples.\(plot.id)")
    }

    private var controls: some View {
        VStack(alignment: .leading, spacing: 10) {
            if let layer = plot.allLayers.first(where: { $0.kind == .scatter }) {
                SliderRow(
                    title: "Marker: \(layer.title)",
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

            if let layer = plot.allLayers.first(where: { $0.kind == .line }) {
                SliderRow(
                    title: "Fit line",
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

            if let layer = plot.allLayers.first {
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

            if let rasterLayer = plot.allLayers.first(where: { $0.raster != nil }) {
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

    private var sampleCountSummary: String {
        if let pointCloudSummary {
            return pointCloudSummary
        }

        let display = plot.allLayers.reduce(0) { total, layer in total + layer.dataProfile.displaySampleCount }
        let source = plot.allLayers.reduce(UInt64(0)) { total, layer in total + layer.dataProfile.sourceSampleCount }
        if source > UInt64(display) {
            return "\(formattedCount(display)) / \(formattedCount(source)) src"
        }
        return "\(formattedCount(display)) pts"
    }

    private var pointCloudSummary: String? {
        let pointClouds = plot.allLayers.reduce(0) { total, layer in
            total + (layer.pointCloud?.count ?? 0)
        }
        guard pointClouds > 0 else { return nil }

        let occupiedPixels = plot.allLayers.reduce(0) { total, layer in
            total + occupiedPixelCount(for: layer)
        }
        guard occupiedPixels > 0 else {
            return "\(formattedCount(pointClouds)) points"
        }
        return "\(formattedCount(pointClouds)) points, \(formattedCount(occupiedPixels)) occupied pixels"
    }

    private func occupiedPixelCount(for layer: WorkbenchPlotLayer) -> Int {
        guard let pointCloud = layer.pointCloud else {
            return layer.pointRaster?.occupiedPixelCount ?? 0
        }
        let fallback = layer.pointRaster?.occupiedPixelCount ?? 0
        guard
            plotCanvasSize.width > 0,
            plotCanvasSize.height > 0,
            let xAxis = plot.allAxes.first(where: { $0.id == layer.xAxisID }),
            let yAxis = plot.allAxes.first(where: { $0.id == layer.yAxisID })
        else {
            return fallback
        }

        let plotRect = WorkbenchPlotLayout.plotRect(for: plotCanvasSize)
        let rasterSize = WorkbenchPlotLayout.pointRasterSize(for: plotRect)
        if
            let pointRaster = layer.pointRaster,
            pointRaster.width == rasterSize.width,
            pointRaster.height == rasterSize.height,
            pointRaster.xRange == xAxis.range,
            pointRaster.yRange == yAxis.range
        {
            return pointRaster.occupiedPixelCount
        }
        return pointRasterSummaryCache.raster(
            plotFingerprint: plot.dataFingerprint,
            layerID: layer.id,
            pointCloud: pointCloud,
            xRange: xAxis.range,
            yRange: yAxis.range,
            width: rasterSize.width,
            height: rasterSize.height,
            xFootprintDataWidth: workbenchPointRasterXFootprintDataWidth(for: layer)
        ).occupiedPixelCount
    }

    private func formattedCount(_ count: Int) -> String {
        formattedCount(UInt64(count))
    }

    private func formattedCount(_ count: UInt64) -> String {
        if count >= 1_000_000 {
            return String(format: "%.1fM", Double(count) / 1_000_000)
        }
        if count >= 1_000 {
            return String(format: "%.1fk", Double(count) / 1_000)
        }
        return "\(count)"
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
                .frame(width: 122, alignment: .leading)
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

private struct PlotCanvasSizePreferenceKey: PreferenceKey {
    static var defaultValue: CGSize = .zero

    static func reduce(value: inout CGSize, nextValue: () -> CGSize) {
        value = nextValue()
    }
}

private enum WorkbenchPlotLayout {
    static func plotRect(for size: CGSize) -> CGRect {
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

    static func pointRasterSize(for plotRect: CGRect) -> (width: Int, height: Int) {
        (
            width: max(64, min(2_048, Int(plotRect.width.rounded(.up)))),
            height: max(64, min(2_048, Int(plotRect.height.rounded(.up))))
        )
    }
}

private func workbenchPointRasterXFootprintDataWidth(for layer: WorkbenchPlotLayer) -> Double {
    guard layer.dataProfile.strategy == .channelBinPointRaster else {
        return 0
    }
    return layer.dataProfile.xBinWidth ?? 0
}

struct WorkbenchPlotView: View {
    let plot: WorkbenchPlotDocument
    @StateObject private var pointRasterCache = WorkbenchPointRasterCache()

    var body: some View {
        Canvas { context, size in
            if plot.panels.isEmpty {
                let plotRect = plotRect(for: size)
                drawPlot(
                    axes: plot.axes,
                    layers: plot.layers,
                    annotations: plot.annotations,
                    overlayShapes: plot.overlayShapes,
                    title: nil,
                    showLegend: plot.showLegend,
                    in: &context,
                    size: size,
                    plotRect: plotRect
                )
            } else {
                context.fill(Path(CGRect(origin: .zero, size: size)), with: .color(Color(nsColor: .textBackgroundColor)))
                for (index, panel) in plot.panels.enumerated() {
                    let bounds = panelBounds(index: index, count: plot.panels.count, size: size)
                    let plotRect = WorkbenchPlotLayout.plotRect(for: bounds.size).offsetBy(dx: bounds.minX, dy: bounds.minY)
                    drawPlot(
                        axes: panel.axes,
                        layers: panel.layers,
                        annotations: panel.annotations,
                        overlayShapes: panel.overlayShapes,
                        title: panel.title,
                        showLegend: plot.showLegend,
                        in: &context,
                        size: bounds.size,
                        plotRect: plotRect
                    )
                }
            }
        }
        .background(Color(nsColor: .textBackgroundColor))
    }

    private func plotRect(for size: CGSize) -> CGRect {
        WorkbenchPlotLayout.plotRect(for: size)
    }

    private func panelBounds(index: Int, count: Int, size: CGSize) -> CGRect {
        let rowHeight = size.height / Double(max(1, count))
        return CGRect(x: 0, y: rowHeight * Double(index), width: size.width, height: rowHeight)
    }

    private func drawPlot(
        axes: [WorkbenchPlotAxis],
        layers: [WorkbenchPlotLayer],
        annotations: [WorkbenchPlotAnnotation],
        overlayShapes: [WorkbenchPlotOverlayShape],
        title: String?,
        showLegend: Bool,
        in context: inout GraphicsContext,
        size: CGSize,
        plotRect: CGRect
    ) {
        drawBackground(in: &context, size: size, plotRect: plotRect)
        if let title {
            context.draw(Text(title).font(.caption).foregroundColor(.secondary), at: CGPoint(x: plotRect.minX, y: plotRect.minY - 12), anchor: .leading)
        }
        drawRasterLayers(layers, in: &context, plotRect: plotRect)
        drawVectorLayers(layers, axes: axes, in: &context, plotRect: plotRect)
        drawOverlayShapes(overlayShapes, axes: axes, in: &context, plotRect: plotRect)
        drawAnnotations(annotations, axes: axes, in: &context, plotRect: plotRect)
        drawAxes(axes, in: &context, size: size, plotRect: plotRect)
        if showLegend {
            drawLegend(for: layers, in: &context, plotRect: plotRect)
        }
    }

    private func drawBackground(in context: inout GraphicsContext, size: CGSize, plotRect: CGRect) {
        context.fill(Path(CGRect(x: plotRect.minX - 64, y: plotRect.minY - 26, width: size.width, height: size.height)), with: .color(Color(nsColor: .textBackgroundColor)))
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

    private func drawRasterLayers(_ layers: [WorkbenchPlotLayer], in context: inout GraphicsContext, plotRect: CGRect) {
        for layer in layers where layer.kind == .raster && layer.style.visible {
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

    private func drawVectorLayers(
        _ layers: [WorkbenchPlotLayer],
        axes: [WorkbenchPlotAxis],
        in context: inout GraphicsContext,
        plotRect: CGRect
    ) {
        for layer in layers where layer.style.visible {
            guard
                let xAxis = axes.first(where: { $0.id == layer.xAxisID }),
                let yAxis = axes.first(where: { $0.id == layer.yAxisID })
            else {
                continue
            }
            switch layer.kind {
            case .scatter:
                if let pointRaster = pointRaster(for: layer, xAxis: xAxis, yAxis: yAxis, plotRect: plotRect) {
                    drawPointRaster(pointRaster, layer: layer, in: &context, plotRect: plotRect)
                    continue
                }
                let color = color(hex: layer.style.colorHex).opacity(layer.style.opacity)
                for point in renderPoints(for: layer) {
                    guard let position = screenPoint(point, xAxis: xAxis, yAxis: yAxis, plotRect: plotRect) else { continue }
                    let radius = max(0.5, (point.symbolSize ?? layer.style.symbolSize) / 2)
                    context.fill(
                        Path(ellipseIn: CGRect(
                            x: position.x - radius,
                            y: position.y - radius,
                            width: radius * 2,
                            height: radius * 2
                        )),
                        with: .color(color)
                    )
                    drawPointDecorations(point, position: position, layer: layer, in: &context)
                }
            case .line:
                var path = Path()
                var hasSubpath = false
                var drewSegment = false
                for point in renderPoints(for: layer) {
                    guard let position = screenPoint(point, xAxis: xAxis, yAxis: yAxis, plotRect: plotRect) else {
                        hasSubpath = false
                        continue
                    }
                    if !hasSubpath || point.lineBreakBefore {
                        path.move(to: position)
                        hasSubpath = true
                    } else {
                        path.addLine(to: position)
                        drewSegment = true
                    }
                    drawPointDecorations(point, position: position, layer: layer, in: &context)
                }
                guard drewSegment else { continue }
                context.stroke(
                    path,
                    with: .color(color(hex: layer.style.colorHex).opacity(layer.style.opacity)),
                    lineWidth: layer.style.lineWidth
                )
            case .interval:
                drawIntervals(layer.intervals, layer: layer, xAxis: xAxis, yAxis: yAxis, in: &context, plotRect: plotRect)
            case .raster:
                continue
            }
        }
    }

    private func drawPointRaster(
        _ pointRaster: WorkbenchPlotPointRaster,
        layer: WorkbenchPlotLayer,
        in context: inout GraphicsContext,
        plotRect: CGRect
    ) {
        guard let image = pointRasterImage(pointRaster, layer: layer) else {
            return
        }
        context.draw(
            Image(decorative: image, scale: 1, orientation: .up),
            in: plotRect
        )
    }

    private func drawPointDecorations(
        _ point: WorkbenchPlotPoint,
        position: CGPoint,
        layer: WorkbenchPlotLayer,
        in context: inout GraphicsContext
    ) {
        if point.selected {
            let radius = max(4, (point.symbolSize ?? layer.style.symbolSize) + 3)
            var path = Path()
            path.move(to: CGPoint(x: position.x - radius, y: position.y))
            path.addLine(to: CGPoint(x: position.x + radius, y: position.y))
            path.move(to: CGPoint(x: position.x, y: position.y - radius))
            path.addLine(to: CGPoint(x: position.x, y: position.y + radius))
            context.stroke(path, with: .color(.primary.opacity(0.9)), lineWidth: 1.4)
        }
        guard let label = point.label else { return }
        context.draw(Text(label).font(.caption2), at: CGPoint(x: position.x + 6, y: position.y - 6), anchor: .leading)
    }

    private func drawIntervals(
        _ intervals: [WorkbenchPlotInterval],
        layer: WorkbenchPlotLayer,
        xAxis: WorkbenchPlotAxis,
        yAxis: WorkbenchPlotAxis,
        in context: inout GraphicsContext,
        plotRect: CGRect
    ) {
        let fill = color(hex: layer.style.colorHex).opacity(layer.style.opacity)
        for interval in intervals {
            let lowerPoint = WorkbenchPlotPoint(x: interval.xStart, y: interval.y - interval.height / 2)
            let upperPoint = WorkbenchPlotPoint(x: interval.xEnd, y: interval.y + interval.height / 2)
            guard
                let lower = screenPoint(lowerPoint, xAxis: xAxis, yAxis: yAxis, plotRect: plotRect),
                let upper = screenPoint(upperPoint, xAxis: xAxis, yAxis: yAxis, plotRect: plotRect)
            else {
                continue
            }
            let rect = CGRect(
                x: min(lower.x, upper.x),
                y: min(lower.y, upper.y),
                width: max(1, abs(upper.x - lower.x)),
                height: max(3, abs(upper.y - lower.y))
            )
            context.fill(Path(roundedRect: rect, cornerRadius: 3), with: .color(fill))
            if let label = interval.label, rect.width > 42 {
                context.draw(Text(label).font(.caption2), at: CGPoint(x: rect.midX, y: rect.midY), anchor: .center)
            }
        }
    }

    private func drawOverlayShapes(
        _ overlayShapes: [WorkbenchPlotOverlayShape],
        axes: [WorkbenchPlotAxis],
        in context: inout GraphicsContext,
        plotRect: CGRect
    ) {
        guard let xAxis = axes.first, let yAxis = axes.dropFirst().first else { return }
        for shape in overlayShapes where shape.points.count > 1 && shape.style.visible {
            let positions = shape.points.compactMap { screenPoint($0, xAxis: xAxis, yAxis: yAxis, plotRect: plotRect) }
            guard positions.count > 1 else { continue }
            var path = Path()
            path.move(to: positions[0])
            for point in positions.dropFirst() {
                path.addLine(to: point)
            }
            if shape.closed {
                path.closeSubpath()
            }
            context.stroke(
                path,
                with: .color(color(hex: shape.style.colorHex).opacity(shape.style.opacity)),
                lineWidth: shape.style.lineWidth
            )
            if let label = shape.label {
                context.draw(Text(label).font(.caption2), at: positions[0], anchor: .bottomLeading)
            }
        }
    }

    private func drawAnnotations(
        _ annotations: [WorkbenchPlotAnnotation],
        axes: [WorkbenchPlotAxis],
        in context: inout GraphicsContext,
        plotRect: CGRect
    ) {
        guard let xAxis = axes.first, let yAxis = axes.dropFirst().first else { return }
        for annotation in annotations {
            let point = WorkbenchPlotPoint(x: annotation.x, y: annotation.y)
            guard let position = screenPoint(point, xAxis: xAxis, yAxis: yAxis, plotRect: plotRect) else { continue }
            let label = Text(annotation.text).font(.caption)
            context.draw(label, at: CGPoint(x: position.x + 6, y: position.y - 8), anchor: .leading)
            context.fill(
                Path(ellipseIn: CGRect(x: position.x - 2, y: position.y - 2, width: 4, height: 4)),
                with: .color(.primary)
            )
        }
    }

    private func drawAxes(_ axes: [WorkbenchPlotAxis], in context: inout GraphicsContext, size: CGSize, plotRect: CGRect) {
        let frame = Path(plotRect)
        context.stroke(frame, with: .color(Color.secondary.opacity(0.45)), lineWidth: 1)

        if let xAxis = axes.first {
            drawTicks(axis: xAxis, horizontal: true, in: &context, plotRect: plotRect)
            if xAxis.labelsVisible {
                context.draw(
                    Text(axisLabel(xAxis)).font(.caption),
                    at: CGPoint(x: plotRect.midX, y: plotRect.maxY + 38),
                    anchor: .center
                )
            }
        }
        for yAxis in axes.dropFirst() {
            drawTicks(axis: yAxis, horizontal: false, in: &context, plotRect: plotRect)
            if yAxis.labelsVisible {
                context.draw(
                    Text(axisLabel(yAxis)).font(.caption),
                    at: CGPoint(x: yAxis.drawsOnTrailingEdge ? plotRect.maxX + 12 : plotRect.minX - 52, y: plotRect.midY),
                    anchor: yAxis.drawsOnTrailingEdge ? .leading : .leading
                )
            }
        }
    }

    private func drawLegend(
        for layers: [WorkbenchPlotLayer],
        in context: inout GraphicsContext,
        plotRect: CGRect
    ) {
        let visibleLayers = layers
            .filter { $0.style.visible }
            .filter { !$0.title.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty }
        guard visibleLayers.count > 1 else { return }

        let displayedLayers = Array(visibleLayers.prefix(8))
        let rowHeight = 16.0
        let legendWidth = min(260.0, max(150.0, plotRect.width * 0.32))
        let legendHeight = rowHeight * Double(displayedLayers.count) + (visibleLayers.count > displayedLayers.count ? rowHeight : 0) + 10
        let legendRect = CGRect(
            x: plotRect.maxX - legendWidth - 8,
            y: plotRect.minY + 8,
            width: legendWidth,
            height: legendHeight
        )
        context.fill(Path(roundedRect: legendRect, cornerRadius: 5), with: .color(Color(nsColor: .textBackgroundColor).opacity(0.82)))
        context.stroke(Path(roundedRect: legendRect, cornerRadius: 5), with: .color(Color.secondary.opacity(0.22)), lineWidth: 1)

        for (index, layer) in displayedLayers.enumerated() {
            let y = legendRect.minY + 10 + Double(index) * rowHeight
            let color = color(hex: layer.style.colorHex).opacity(layer.style.opacity)
            var marker = Path()
            let markerY = y + 5
            switch layer.kind {
            case .line:
                marker.move(to: CGPoint(x: legendRect.minX + 9, y: markerY))
                marker.addLine(to: CGPoint(x: legendRect.minX + 23, y: markerY))
                context.stroke(marker, with: .color(color), lineWidth: max(1.4, layer.style.lineWidth))
            case .interval:
                let rect = CGRect(x: legendRect.minX + 9, y: markerY - 4, width: 14, height: 8)
                context.fill(Path(roundedRect: rect, cornerRadius: 2), with: .color(color))
            case .scatter, .raster:
                let radius = max(2.0, min(5.0, layer.style.symbolSize / 2))
                context.fill(
                    Path(ellipseIn: CGRect(x: legendRect.minX + 16 - radius, y: markerY - radius, width: radius * 2, height: radius * 2)),
                    with: .color(color)
                )
            }
            context.draw(
                Text(layer.title).font(.caption2).foregroundColor(.secondary),
                at: CGPoint(x: legendRect.minX + 30, y: y),
                anchor: .topLeading
            )
        }

        if visibleLayers.count > displayedLayers.count {
            let remaining = visibleLayers.count - displayedLayers.count
            context.draw(
                Text("+ \(remaining) more").font(.caption2).foregroundColor(.secondary),
                at: CGPoint(x: legendRect.minX + 30, y: legendRect.minY + 10 + Double(displayedLayers.count) * rowHeight),
                anchor: .topLeading
            )
        }
    }

    private func drawTicks(
        axis: WorkbenchPlotAxis,
        horizontal: Bool,
        in context: inout GraphicsContext,
        plotRect: CGRect
    ) {
        for fraction in stride(from: 0.0, through: 1.0, by: 0.25) {
            let value = axisValue(at: fraction, axis: axis)
            let text = Text(shortNumber(value)).font(.caption2)
            if horizontal {
                let x = plotRect.minX + plotRect.width * fraction
                context.draw(text, at: CGPoint(x: x, y: plotRect.maxY + 16), anchor: .center)
            } else {
                let y = plotRect.maxY - plotRect.height * fraction
                let x = axis.drawsOnTrailingEdge ? plotRect.maxX + 8 : plotRect.minX - 8
                context.draw(text, at: CGPoint(x: x, y: y), anchor: axis.drawsOnTrailingEdge ? .leading : .trailing)
            }
        }
        guard !horizontal, !axis.laneLabels.isEmpty else { return }
        for (index, label) in axis.laneLabels.enumerated() {
            let value = Double(index)
            let fraction = (value - axis.range.lower) / axis.range.span
            guard fraction.isFinite else { continue }
            let y = plotRect.maxY - plotRect.height * fraction
            context.draw(Text(label).font(.caption2), at: CGPoint(x: plotRect.minX - 8, y: y), anchor: .trailing)
        }
    }

    private func screenPoint(
        _ point: WorkbenchPlotPoint,
        xAxis: WorkbenchPlotAxis,
        yAxis: WorkbenchPlotAxis,
        plotRect: CGRect
    ) -> CGPoint? {
        guard xAxis.range.span != 0, yAxis.range.span != 0 else {
            return nil
        }
        guard let x = axisFraction(for: point.x, axis: xAxis),
              let y = axisFraction(for: point.y, axis: yAxis) else {
            return nil
        }
        guard x.isFinite, y.isFinite else { return nil }
        return CGPoint(
            x: plotRect.minX + plotRect.width * x,
            y: plotRect.maxY - plotRect.height * y
        )
    }

    private func renderPoints(for layer: WorkbenchPlotLayer) -> [WorkbenchPlotPoint] {
        let pointLimit = max(1, min(layer.dataProfile.pointBudget, 50_000))
        if layer.points.isEmpty, let pointCloud = layer.pointCloud {
            return pointCloud.sampledPoints(limit: pointLimit)
        }
        guard layer.points.count > pointLimit else {
            return layer.points
        }
        guard pointLimit > 1 else {
            return Array(layer.points.prefix(1))
        }
        let step = Double(layer.points.count - 1) / Double(pointLimit - 1)
        return (0..<pointLimit).map { index in
            layer.points[Int((Double(index) * step).rounded())]
        }
    }

    private func pointRaster(
        for layer: WorkbenchPlotLayer,
        xAxis: WorkbenchPlotAxis,
        yAxis: WorkbenchPlotAxis,
        plotRect: CGRect
    ) -> WorkbenchPlotPointRaster? {
        let rasterSize = pointRasterSize(for: plotRect)
        if
            let pointRaster = layer.pointRaster,
            pointRaster.width == rasterSize.width,
            pointRaster.height == rasterSize.height,
            pointRaster.xRange == xAxis.range,
            pointRaster.yRange == yAxis.range
        {
            return pointRaster
        }
        guard
            let pointCloud = layer.pointCloud,
            pointCloud.count > layer.dataProfile.pointBudget
        else {
            return nil
        }
        return pointRaster(
            from: pointCloud,
            layer: layer,
            xAxis: xAxis,
            yAxis: yAxis,
            size: rasterSize
        )
    }

    private func pointRaster(
        from pointCloud: WorkbenchPlotPointCloud,
        layer: WorkbenchPlotLayer,
        xAxis: WorkbenchPlotAxis,
        yAxis: WorkbenchPlotAxis,
        size: (width: Int, height: Int)
    ) -> WorkbenchPlotPointRaster {
        pointRasterCache.raster(
            plotFingerprint: plot.dataFingerprint,
            layerID: layer.id,
            pointCloud: pointCloud,
            xRange: xAxis.range,
            yRange: yAxis.range,
            width: size.width,
            height: size.height,
            xFootprintDataWidth: workbenchPointRasterXFootprintDataWidth(for: layer)
        )
    }

    private func pointRasterSize(for plotRect: CGRect) -> (width: Int, height: Int) {
        WorkbenchPlotLayout.pointRasterSize(for: plotRect)
    }

    private func pointRasterImage(_ pointRaster: WorkbenchPlotPointRaster, layer: WorkbenchPlotLayer) -> CGImage? {
        guard pointRaster.maxCount > 0, pointRaster.width > 0, pointRaster.height > 0 else { return nil }
        let components = rgbaComponents(hex: layer.style.colorHex, opacity: layer.style.opacity)
        let bytesPerPixel = 4
        let bytesPerRow = pointRaster.width * bytesPerPixel
        var pixels = Array(repeating: UInt8(0), count: pointRaster.height * bytesPerRow)
        let markerSize = pointRasterMarkerSize(for: layer)
        let lowerRadius = (markerSize - 1) / 2
        let upperRadius = markerSize / 2
        let occupancyPrefix = markerSize > 1 ? pointRasterOccupancyPrefix(pointRaster) : []

        for y in 0..<pointRaster.height {
            let outputY = pointRaster.height - 1 - y
            for x in 0..<pointRaster.width {
                if markerSize == 1 {
                    guard pointRaster.countAt(x: x, y: y) > 0 else { continue }
                } else {
                    guard pointRasterHasPoint(
                        occupancyPrefix: occupancyPrefix,
                        width: pointRaster.width,
                        height: pointRaster.height,
                        nearX: x,
                        y: y,
                        lowerRadius: lowerRadius,
                        upperRadius: upperRadius
                    ) else { continue }
                }
                let offset = outputY * bytesPerRow + x * bytesPerPixel
                pixels[offset] = components.red
                pixels[offset + 1] = components.green
                pixels[offset + 2] = components.blue
                pixels[offset + 3] = components.alpha
            }
        }

        let data = Data(pixels)
        guard
            let provider = CGDataProvider(data: data as CFData),
            let colorSpace = CGColorSpace(name: CGColorSpace.sRGB)
        else {
            return nil
        }
        return CGImage(
            width: pointRaster.width,
            height: pointRaster.height,
            bitsPerComponent: 8,
            bitsPerPixel: 32,
            bytesPerRow: bytesPerRow,
            space: colorSpace,
            bitmapInfo: CGBitmapInfo(rawValue: CGImageAlphaInfo.premultipliedLast.rawValue),
            provider: provider,
            decode: nil,
            shouldInterpolate: false,
            intent: .defaultIntent
        )
    }

    private func pointRasterMarkerSize(for layer: WorkbenchPlotLayer) -> Int {
        max(1, min(24, Int(layer.style.symbolSize.rounded(.toNearestOrAwayFromZero))))
    }

    private func pointRasterOccupancyPrefix(_ pointRaster: WorkbenchPlotPointRaster) -> [Int] {
        let prefixWidth = pointRaster.width + 1
        var prefix = Array(repeating: 0, count: prefixWidth * (pointRaster.height + 1))
        for y in 0..<pointRaster.height {
            var rowTotal = 0
            for x in 0..<pointRaster.width {
                if pointRaster.counts[y * pointRaster.width + x] > 0 {
                    rowTotal += 1
                }
                prefix[(y + 1) * prefixWidth + x + 1] = prefix[y * prefixWidth + x + 1] + rowTotal
            }
        }
        return prefix
    }

    private func pointRasterHasPoint(
        occupancyPrefix: [Int],
        width: Int,
        height: Int,
        nearX x: Int,
        y: Int,
        lowerRadius: Int,
        upperRadius: Int
    ) -> Bool {
        let prefixWidth = width + 1
        guard occupancyPrefix.count == prefixWidth * (height + 1) else { return false }
        let minX = max(0, x - lowerRadius)
        let maxX = min(width - 1, x + upperRadius)
        let minY = max(0, y - lowerRadius)
        let maxY = min(height - 1, y + upperRadius)
        let x0 = minX
        let x1 = maxX + 1
        let y0 = minY
        let y1 = maxY + 1
        let occupied = occupancyPrefix[y1 * prefixWidth + x1]
            - occupancyPrefix[y0 * prefixWidth + x1]
            - occupancyPrefix[y1 * prefixWidth + x0]
            + occupancyPrefix[y0 * prefixWidth + x0]
        return occupied > 0
    }

    private func axisLabel(_ axis: WorkbenchPlotAxis) -> String {
        let label = axis.unit.isEmpty ? axis.label : "\(axis.label) (\(axis.unit))"
        return axis.scale == .logarithmic ? "\(label), log" : label
    }

    private func axisFraction(for value: Double, axis: WorkbenchPlotAxis) -> Double? {
        switch axis.scale {
        case .linear:
            guard axis.range.span != 0 else { return nil }
            return (value - axis.range.lower) / axis.range.span
        case .logarithmic:
            guard value > 0, axis.range.lower > 0, axis.range.upper > 0 else { return nil }
            let lower = log10(axis.range.lower)
            let upper = log10(axis.range.upper)
            guard upper != lower else { return nil }
            return (log10(value) - lower) / (upper - lower)
        }
    }

    private func axisValue(at fraction: Double, axis: WorkbenchPlotAxis) -> Double {
        switch axis.scale {
        case .linear:
            return axis.range.lower + axis.range.span * fraction
        case .logarithmic:
            guard axis.range.lower > 0, axis.range.upper > 0 else {
                return axis.range.lower + axis.range.span * fraction
            }
            let lower = log10(axis.range.lower)
            let upper = log10(axis.range.upper)
            return pow(10, lower + (upper - lower) * fraction)
        }
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
        let components = rgbComponents(hex: hex)
        return Color(red: components.red, green: components.green, blue: components.blue)
    }

    private func rgbaComponents(hex: String, opacity: Double) -> (red: UInt8, green: UInt8, blue: UInt8, alpha: UInt8) {
        let components = rgbComponents(hex: hex)
        let alpha = min(1, max(0, opacity))
        return (
            red: UInt8((components.red * alpha * 255).rounded()),
            green: UInt8((components.green * alpha * 255).rounded()),
            blue: UInt8((components.blue * alpha * 255).rounded()),
            alpha: UInt8((alpha * 255).rounded())
        )
    }

    private func rgbComponents(hex: String) -> (red: Double, green: Double, blue: Double) {
        let trimmed = hex.trimmingCharacters(in: CharacterSet(charactersIn: "#"))
        guard trimmed.count == 6, let value = UInt64(trimmed, radix: 16) else {
            return (0, 0.48, 1)
        }
        return (
            red: Double((value >> 16) & 0xff) / 255,
            green: Double((value >> 8) & 0xff) / 255,
            blue: Double(value & 0xff) / 255
        )
    }
}

private final class WorkbenchPointRasterCache: ObservableObject {
    private struct Key: Hashable {
        var plotFingerprint: String
        var layerID: String
        var width: Int
        var height: Int
        var xLower: UInt64
        var xUpper: UInt64
        var yLower: UInt64
        var yUpper: UInt64
        var xFootprintDataWidth: UInt64
    }

    private var rasters: [Key: WorkbenchPlotPointRaster] = [:]

    func raster(
        plotFingerprint: String,
        layerID: String,
        pointCloud: WorkbenchPlotPointCloud,
        xRange: WorkbenchPlotRange,
        yRange: WorkbenchPlotRange,
        width: Int,
        height: Int,
        xFootprintDataWidth: Double = 0
    ) -> WorkbenchPlotPointRaster {
        let key = Key(
            plotFingerprint: plotFingerprint,
            layerID: layerID,
            width: width,
            height: height,
            xLower: xRange.lower.bitPattern,
            xUpper: xRange.upper.bitPattern,
            yLower: yRange.lower.bitPattern,
            yUpper: yRange.upper.bitPattern,
            xFootprintDataWidth: xFootprintDataWidth.bitPattern
        )
        if let cached = rasters[key] {
            return cached
        }
        let raster = WorkbenchPlotPointRaster.build(
            from: pointCloud,
            xRange: xRange,
            yRange: yRange,
            width: width,
            height: height,
            xFootprintDataWidth: xFootprintDataWidth
        )
        if rasters.count >= 8 {
            rasters.removeAll(keepingCapacity: true)
        }
        rasters[key] = raster
        return raster
    }
}
