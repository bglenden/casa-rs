import Foundation

public enum WorkbenchPlotLayerKind: String, Codable, Equatable {
    case scatter
    case line
    case interval
    case raster
}

public enum WorkbenchPlotPayloadStrategy: String, Codable, Equatable {
    case inlineDisplayPoints
    case viewportLevelOfDetail
    case singlePixelPointRaster
    case channelBinPointRaster
    case densityGrid
    case rasterOverview
}

public struct WorkbenchPlotLayerDataProfile: Codable, Equatable {
    public var sourceSampleCount: UInt64
    public var displaySampleCount: Int
    public var pointBudget: Int
    public var strategy: WorkbenchPlotPayloadStrategy
    public var sourceDescription: String
    public var provenanceKey: String?
    public var xBinWidth: Double?

    public init(
        sourceSampleCount: UInt64,
        displaySampleCount: Int,
        pointBudget: Int = 50_000,
        strategy: WorkbenchPlotPayloadStrategy,
        sourceDescription: String,
        provenanceKey: String? = nil,
        xBinWidth: Double? = nil
    ) {
        self.sourceSampleCount = sourceSampleCount
        self.displaySampleCount = displaySampleCount
        self.pointBudget = pointBudget
        self.strategy = strategy
        self.sourceDescription = sourceDescription
        self.provenanceKey = provenanceKey
        self.xBinWidth = xBinWidth
    }

    public var isDisplayPayloadBounded: Bool {
        displaySampleCount <= pointBudget && UInt64(displaySampleCount) <= sourceSampleCount
    }
}

public enum WorkbenchPlotImageStretch: String, CaseIterable, Codable, Equatable, Identifiable {
    case linear
    case squareRoot
    case logarithmic
    case percentile

    public var id: String { rawValue }
}

public enum WorkbenchPlotColorMap: String, CaseIterable, Codable, Equatable, Identifiable {
    case viridis
    case magma
    case grayscale
    case coolWarm

    public var id: String { rawValue }
}

public struct WorkbenchPlotRange: Codable, Equatable {
    public var lower: Double
    public var upper: Double

    public init(lower: Double, upper: Double) {
        self.lower = lower
        self.upper = upper
    }

    public var span: Double {
        upper - lower
    }
}

public enum WorkbenchPlotAxisScale: String, Codable, Equatable {
    case linear
    case logarithmic
}

public struct WorkbenchPlotAxis: Identifiable, Codable, Equatable {
    public let id: String
    public var label: String
    public var unit: String
    public var range: WorkbenchPlotRange
    public var scale: WorkbenchPlotAxisScale
    public var laneLabels: [String]
    public var drawsOnTrailingEdge: Bool
    public var labelsVisible: Bool
    public var gridVisible: Bool

    public init(
        id: String,
        label: String,
        unit: String,
        range: WorkbenchPlotRange,
        scale: WorkbenchPlotAxisScale = .linear,
        laneLabels: [String] = [],
        drawsOnTrailingEdge: Bool = false,
        labelsVisible: Bool = true,
        gridVisible: Bool = true
    ) {
        self.id = id
        self.label = label
        self.unit = unit
        self.range = range
        self.scale = scale
        self.laneLabels = laneLabels
        self.drawsOnTrailingEdge = drawsOnTrailingEdge
        self.labelsVisible = labelsVisible
        self.gridVisible = gridVisible
    }
}

public struct WorkbenchPlotPointProvenance: Codable, Equatable {
    public var row: UInt64?
    public var field: String?
    public var spectralWindow: String?
    public var correlation: String?
    public var source: String?

    public init(
        row: UInt64? = nil,
        field: String? = nil,
        spectralWindow: String? = nil,
        correlation: String? = nil,
        source: String? = nil
    ) {
        self.row = row
        self.field = field
        self.spectralWindow = spectralWindow
        self.correlation = correlation
        self.source = source
    }
}

public struct WorkbenchPlotPoint: Codable, Equatable {
    public var x: Double
    public var y: Double
    public var label: String?
    public var symbolSize: Double?
    public var lineBreakBefore: Bool
    public var selected: Bool
    public var provenance: WorkbenchPlotPointProvenance?

    public init(
        x: Double,
        y: Double,
        label: String? = nil,
        symbolSize: Double? = nil,
        lineBreakBefore: Bool = false,
        selected: Bool = false,
        provenance: WorkbenchPlotPointProvenance? = nil
    ) {
        self.x = x
        self.y = y
        self.label = label
        self.symbolSize = symbolSize
        self.lineBreakBefore = lineBreakBefore
        self.selected = selected
        self.provenance = provenance
    }
}

public struct WorkbenchPlotPointCloud: Codable, Equatable {
    public var xValues: [Double]
    public var yValues: [Double]
    public var provenanceSamples: [WorkbenchPlotPointProvenance]

    public init(
        xValues: [Double],
        yValues: [Double],
        provenanceSamples: [WorkbenchPlotPointProvenance] = []
    ) {
        self.xValues = xValues
        self.yValues = yValues
        self.provenanceSamples = provenanceSamples
    }

    public var count: Int {
        min(xValues.count, yValues.count)
    }

    public var firstPoint: WorkbenchPlotPoint? {
        guard count > 0 else { return nil }
        return WorkbenchPlotPoint(x: xValues[0], y: yValues[0], provenance: provenanceSamples.first)
    }

    public var lastPoint: WorkbenchPlotPoint? {
        guard count > 0 else { return nil }
        return WorkbenchPlotPoint(x: xValues[count - 1], y: yValues[count - 1], provenance: provenanceSamples.last)
    }

    public func sampledPoints(limit: Int) -> [WorkbenchPlotPoint] {
        let boundedLimit = max(0, limit)
        guard count > 0, boundedLimit > 0 else { return [] }
        guard count > boundedLimit else {
            return (0..<count).map { index in
                WorkbenchPlotPoint(x: xValues[index], y: yValues[index])
            }
        }
        guard boundedLimit > 1 else {
            return [WorkbenchPlotPoint(x: xValues[0], y: yValues[0])]
        }
        let step = Double(count - 1) / Double(boundedLimit - 1)
        return (0..<boundedLimit).map { outputIndex in
            let sourceIndex = Int((Double(outputIndex) * step).rounded())
            return WorkbenchPlotPoint(x: xValues[sourceIndex], y: yValues[sourceIndex])
        }
    }
}

public struct WorkbenchPlotPointRaster: Codable, Equatable {
    public var width: Int
    public var height: Int
    public var counts: [UInt32]
    public var maxCount: UInt32
    public var totalCount: UInt64
    public var xRange: WorkbenchPlotRange
    public var yRange: WorkbenchPlotRange

    public init(
        width: Int,
        height: Int,
        counts: [UInt32],
        totalCount: UInt64? = nil,
        xRange: WorkbenchPlotRange,
        yRange: WorkbenchPlotRange
    ) {
        let boundedWidth = max(1, width)
        let boundedHeight = max(1, height)
        let expectedCount = boundedWidth * boundedHeight
        let boundedCounts: [UInt32]
        if counts.count == expectedCount {
            boundedCounts = counts
        } else if counts.count > expectedCount {
            boundedCounts = Array(counts.prefix(expectedCount))
        } else {
            boundedCounts = counts + Array(repeating: 0, count: expectedCount - counts.count)
        }
        self.width = boundedWidth
        self.height = boundedHeight
        self.counts = boundedCounts
        self.maxCount = boundedCounts.max() ?? 0
        self.totalCount = totalCount ?? boundedCounts.reduce(UInt64(0)) { total, count in
            total + UInt64(count)
        }
        self.xRange = xRange
        self.yRange = yRange
    }

    public var nonEmptyBinCount: Int {
        counts.reduce(0) { total, count in total + (count > 0 ? 1 : 0) }
    }

    public var occupiedPixelCount: Int {
        nonEmptyBinCount
    }

    public func countAt(x: Int, y: Int) -> UInt32 {
        guard x >= 0, x < width, y >= 0, y < height else { return 0 }
        return counts[y * width + x]
    }

    public static func build(
        from pointCloud: WorkbenchPlotPointCloud,
        xRange: WorkbenchPlotRange,
        yRange: WorkbenchPlotRange,
        width: Int,
        height: Int,
        xFootprintDataWidth: Double = 0
    ) -> WorkbenchPlotPointRaster {
        let boundedWidth = max(1, width)
        let boundedHeight = max(1, height)
        let xFootprintDataWidth = xFootprintDataWidth.isFinite ? max(0, xFootprintDataWidth) : 0
        guard xRange.span > 0, yRange.span > 0 else {
            return WorkbenchPlotPointRaster(
                width: boundedWidth,
                height: boundedHeight,
                counts: [],
                totalCount: 0,
                xRange: xRange,
                yRange: yRange
            )
        }

        var counts = Array(repeating: UInt32(0), count: boundedWidth * boundedHeight)
        var totalCount: UInt64 = 0
        for index in 0..<pointCloud.count {
            let x = pointCloud.xValues[index]
            let y = pointCloud.yValues[index]
            guard x.isFinite, y.isFinite else { continue }
            guard x >= xRange.lower, x <= xRange.upper, y >= yRange.lower, y <= yRange.upper else { continue }

            let yFraction = (y - yRange.lower) / yRange.span
            let yBin = min(boundedHeight - 1, max(0, Int((yFraction * Double(boundedHeight)).rounded(.down))))
            let xBins: ClosedRange<Int>
            if xFootprintDataWidth > 0 {
                let lower = max(xRange.lower, x - xFootprintDataWidth / 2)
                let upper = min(xRange.upper, x + xFootprintDataWidth / 2)
                let lowerFraction = (lower - xRange.lower) / xRange.span
                let upperFraction = (upper - xRange.lower) / xRange.span
                let lowerBin = min(
                    boundedWidth - 1,
                    max(0, Int((lowerFraction * Double(boundedWidth)).rounded(.down)))
                )
                let upperBin = min(
                    boundedWidth - 1,
                    max(lowerBin, Int((upperFraction * Double(boundedWidth)).rounded(.up)) - 1)
                )
                xBins = lowerBin...upperBin
            } else {
                let xFraction = (x - xRange.lower) / xRange.span
                let xBin = min(boundedWidth - 1, max(0, Int((xFraction * Double(boundedWidth)).rounded(.down))))
                xBins = xBin...xBin
            }
            for xBin in xBins {
                let binIndex = yBin * boundedWidth + xBin
                if counts[binIndex] < UInt32.max {
                    counts[binIndex] += 1
                }
            }
            totalCount += 1
        }

        return WorkbenchPlotPointRaster(
            width: boundedWidth,
            height: boundedHeight,
            counts: counts,
            totalCount: totalCount,
            xRange: xRange,
            yRange: yRange
        )
    }
}

public struct WorkbenchPlotLayerStyle: Codable, Equatable {
    public var colorHex: String
    public var symbolSize: Double
    public var lineWidth: Double
    public var opacity: Double
    public var visible: Bool

    public init(
        colorHex: String,
        symbolSize: Double = 4,
        lineWidth: Double = 1.5,
        opacity: Double = 1,
        visible: Bool = true
    ) {
        self.colorHex = colorHex
        self.symbolSize = symbolSize
        self.lineWidth = lineWidth
        self.opacity = opacity
        self.visible = visible
    }
}

public struct WorkbenchPlotRaster: Codable, Equatable {
    public var width: Int
    public var height: Int
    public var values: [Double]
    public var valueRange: WorkbenchPlotRange
    public var stretch: WorkbenchPlotImageStretch
    public var colorMap: WorkbenchPlotColorMap
    public var xAxisLabel: String
    public var yAxisLabel: String
    public var colorbarLabel: String

    public init(
        width: Int,
        height: Int,
        values: [Double],
        valueRange: WorkbenchPlotRange,
        stretch: WorkbenchPlotImageStretch,
        colorMap: WorkbenchPlotColorMap,
        xAxisLabel: String,
        yAxisLabel: String,
        colorbarLabel: String
    ) {
        self.width = width
        self.height = height
        self.values = values
        self.valueRange = valueRange
        self.stretch = stretch
        self.colorMap = colorMap
        self.xAxisLabel = xAxisLabel
        self.yAxisLabel = yAxisLabel
        self.colorbarLabel = colorbarLabel
    }
}

public struct WorkbenchPlotInterval: Identifiable, Codable, Equatable {
    public let id: String
    public var xStart: Double
    public var xEnd: Double
    public var y: Double
    public var height: Double
    public var label: String?
    public var provenance: WorkbenchPlotPointProvenance?

    public init(
        id: String,
        xStart: Double,
        xEnd: Double,
        y: Double,
        height: Double = 0.72,
        label: String? = nil,
        provenance: WorkbenchPlotPointProvenance? = nil
    ) {
        self.id = id
        self.xStart = xStart
        self.xEnd = xEnd
        self.y = y
        self.height = height
        self.label = label
        self.provenance = provenance
    }
}

public struct WorkbenchPlotOverlayShape: Identifiable, Codable, Equatable {
    public let id: String
    public var points: [WorkbenchPlotPoint]
    public var closed: Bool
    public var label: String?
    public var style: WorkbenchPlotLayerStyle

    public init(
        id: String,
        points: [WorkbenchPlotPoint],
        closed: Bool = true,
        label: String? = nil,
        style: WorkbenchPlotLayerStyle
    ) {
        self.id = id
        self.points = points
        self.closed = closed
        self.label = label
        self.style = style
    }
}

public struct WorkbenchPlotLayer: Identifiable, Codable, Equatable {
    public let id: String
    public var title: String
    public var kind: WorkbenchPlotLayerKind
    public var xAxisID: String
    public var yAxisID: String
    public var points: [WorkbenchPlotPoint]
    public var intervals: [WorkbenchPlotInterval]
    public var pointCloud: WorkbenchPlotPointCloud?
    public var pointRaster: WorkbenchPlotPointRaster?
    public var raster: WorkbenchPlotRaster?
    public var style: WorkbenchPlotLayerStyle
    public var provenanceSummary: String
    public var dataProfile: WorkbenchPlotLayerDataProfile

    public init(
        id: String,
        title: String,
        kind: WorkbenchPlotLayerKind,
        xAxisID: String,
        yAxisID: String,
        points: [WorkbenchPlotPoint] = [],
        intervals: [WorkbenchPlotInterval] = [],
        pointCloud: WorkbenchPlotPointCloud? = nil,
        pointRaster: WorkbenchPlotPointRaster? = nil,
        raster: WorkbenchPlotRaster? = nil,
        style: WorkbenchPlotLayerStyle,
        provenanceSummary: String,
        dataProfile: WorkbenchPlotLayerDataProfile? = nil
    ) {
        self.id = id
        self.title = title
        self.kind = kind
        self.xAxisID = xAxisID
        self.yAxisID = yAxisID
        self.points = points
        self.intervals = intervals
        self.pointCloud = pointCloud
        self.pointRaster = pointRaster
        self.raster = raster
        self.style = style
        self.provenanceSummary = provenanceSummary
        let defaultSourceCount = max(
            points.count,
            intervals.count,
            pointCloud?.count ?? 0,
            Int(min(UInt64(Int.max), pointRaster?.totalCount ?? 0)),
            raster?.values.count ?? 0
        )
        let defaultDisplayCount = pointRaster?.occupiedPixelCount ?? max(points.count, intervals.count, raster?.values.count ?? 0)
        self.dataProfile = dataProfile ?? WorkbenchPlotLayerDataProfile(
            sourceSampleCount: UInt64(defaultSourceCount),
            displaySampleCount: defaultDisplayCount,
            pointBudget: pointRaster.map { $0.width * $0.height } ?? (kind == .line ? 100_000 : 50_000),
            strategy: kind == .raster ? .rasterOverview : (pointRaster == nil ? .inlineDisplayPoints : .singlePixelPointRaster),
            sourceDescription: provenanceSummary
        )
    }
}

public struct WorkbenchPlotAnnotation: Identifiable, Codable, Equatable {
    public let id: String
    public var x: Double
    public var y: Double
    public var text: String

    public init(id: String, x: Double, y: Double, text: String) {
        self.id = id
        self.x = x
        self.y = y
        self.text = text
    }
}

public struct WorkbenchPlotPanel: Identifiable, Codable, Equatable {
    public let id: String
    public var title: String
    public var axes: [WorkbenchPlotAxis]
    public var layers: [WorkbenchPlotLayer]
    public var annotations: [WorkbenchPlotAnnotation]
    public var overlayShapes: [WorkbenchPlotOverlayShape]

    public init(
        id: String,
        title: String,
        axes: [WorkbenchPlotAxis],
        layers: [WorkbenchPlotLayer],
        annotations: [WorkbenchPlotAnnotation] = [],
        overlayShapes: [WorkbenchPlotOverlayShape] = []
    ) {
        self.id = id
        self.title = title
        self.axes = axes
        self.layers = layers
        self.annotations = annotations
        self.overlayShapes = overlayShapes
    }
}

public enum WorkbenchPlotEditAction: Codable, Equatable {
    case setLayerSymbolSize(layerID: String, size: Double)
    case setLayerLineWidth(layerID: String, width: Double)
    case setLayerOpacity(layerID: String, opacity: Double)
    case setLayerVisibility(layerID: String, visible: Bool)
    case setLayerColor(layerID: String, colorHex: String)
    case setRasterStretch(layerID: String, stretch: WorkbenchPlotImageStretch)
    case setRasterColorMap(layerID: String, colorMap: WorkbenchPlotColorMap)
    case setAxisLabelsVisible(axisID: String, visible: Bool)
    case addAnnotation(id: String, x: Double, y: Double, text: String)
}

public struct WorkbenchPlotDocument: Identifiable, Codable, Equatable {
    public let id: String
    public var title: String
    public var subtitle: String
    public var axes: [WorkbenchPlotAxis]
    public var layers: [WorkbenchPlotLayer]
    public var annotations: [WorkbenchPlotAnnotation]
    public var overlayShapes: [WorkbenchPlotOverlayShape]
    public var panels: [WorkbenchPlotPanel]
    public var showLegend: Bool
    public var styleRevision: UInt64

    public init(
        id: String,
        title: String,
        subtitle: String,
        axes: [WorkbenchPlotAxis],
        layers: [WorkbenchPlotLayer],
        annotations: [WorkbenchPlotAnnotation] = [],
        overlayShapes: [WorkbenchPlotOverlayShape] = [],
        panels: [WorkbenchPlotPanel] = [],
        showLegend: Bool = true,
        styleRevision: UInt64 = 0
    ) {
        self.id = id
        self.title = title
        self.subtitle = subtitle
        self.axes = axes
        self.layers = layers
        self.annotations = annotations
        self.overlayShapes = overlayShapes
        self.panels = panels
        self.showLegend = showLegend
        self.styleRevision = styleRevision
    }

    public var allLayers: [WorkbenchPlotLayer] {
        layers + panels.flatMap(\.layers)
    }

    public var allAxes: [WorkbenchPlotAxis] {
        axes + panels.flatMap(\.axes)
    }

    public var allAnnotations: [WorkbenchPlotAnnotation] {
        annotations + panels.flatMap(\.annotations)
    }

    public var allOverlayShapes: [WorkbenchPlotOverlayShape] {
        overlayShapes + panels.flatMap(\.overlayShapes)
    }

    public var dataFingerprint: String {
        var parts = [
            id,
            allAxes.map { "\($0.id):\($0.range.lower.bitPattern):\($0.range.upper.bitPattern):\($0.scale.rawValue):\($0.laneLabels.joined(separator: "/")):\($0.drawsOnTrailingEdge)" }
                .joined(separator: ",")
        ]
        parts.append("panels:\(panels.count)")
        for layer in allLayers {
            parts.append("\(layer.id):\(layer.kind.rawValue):\(layer.points.count):\(layer.intervals.count):\(layer.pointCloud?.count ?? 0)")
            parts.append(
                "profile:\(layer.dataProfile.sourceSampleCount):\(layer.dataProfile.displaySampleCount):\(layer.dataProfile.strategy.rawValue):\(layer.dataProfile.xBinWidth?.bitPattern ?? 0)"
            )
            if let first = layer.points.first {
                parts.append("first:\(first.x.bitPattern):\(first.y.bitPattern)")
            }
            if let last = layer.points.last {
                parts.append("last:\(last.x.bitPattern):\(last.y.bitPattern)")
            }
            if let first = layer.pointCloud?.firstPoint {
                parts.append("cloud-first:\(first.x.bitPattern):\(first.y.bitPattern)")
            }
            if let last = layer.pointCloud?.lastPoint {
                parts.append("cloud-last:\(last.x.bitPattern):\(last.y.bitPattern)")
            }
            if let pointRaster = layer.pointRaster {
                parts.append(
                    "point-raster:\(pointRaster.width)x\(pointRaster.height):\(pointRaster.totalCount):\(pointRaster.occupiedPixelCount):\(pointRaster.maxCount)"
                )
            }
            if let raster = layer.raster {
                parts.append(
                    "raster:\(raster.width)x\(raster.height):\(raster.values.count):\(raster.valueRange.lower.bitPattern):\(raster.valueRange.upper.bitPattern)"
                )
            }
        }
        for shape in allOverlayShapes {
            parts.append("overlay:\(shape.id):\(shape.points.count):\(shape.closed)")
        }
        return parts.joined(separator: "|")
    }

    public mutating func apply(_ action: WorkbenchPlotEditAction) {
        switch action {
        case let .setLayerSymbolSize(layerID, size):
            updateLayer(layerID) { layer in
                layer.style.symbolSize = Self.clamped(size, 1, 24)
            }
        case let .setLayerLineWidth(layerID, width):
            updateLayer(layerID) { layer in
                layer.style.lineWidth = Self.clamped(width, 0.25, 12)
            }
        case let .setLayerOpacity(layerID, opacity):
            updateLayer(layerID) { layer in
                layer.style.opacity = Self.clamped(opacity, 0, 1)
            }
        case let .setLayerVisibility(layerID, visible):
            updateLayer(layerID) { layer in
                layer.style.visible = visible
            }
        case let .setLayerColor(layerID, colorHex):
            updateLayer(layerID) { layer in
                layer.style.colorHex = colorHex
            }
        case let .setRasterStretch(layerID, stretch):
            updateLayer(layerID) { layer in
                layer.raster?.stretch = stretch
            }
        case let .setRasterColorMap(layerID, colorMap):
            updateLayer(layerID) { layer in
                layer.raster?.colorMap = colorMap
            }
        case let .setAxisLabelsVisible(axisID, visible):
            guard let index = axes.firstIndex(where: { $0.id == axisID }) else { return }
            axes[index].labelsVisible = visible
            styleRevision += 1
        case let .addAnnotation(id, x, y, text):
            annotations.append(WorkbenchPlotAnnotation(id: id, x: x, y: y, text: text))
            styleRevision += 1
        }
    }

    private mutating func updateLayer(_ layerID: String, update: (inout WorkbenchPlotLayer) -> Void) {
        if let index = layers.firstIndex(where: { $0.id == layerID }) {
            update(&layers[index])
            styleRevision += 1
            return
        }
        for panelIndex in panels.indices {
            if let layerIndex = panels[panelIndex].layers.firstIndex(where: { $0.id == layerID }) {
                update(&panels[panelIndex].layers[layerIndex])
                styleRevision += 1
                return
            }
        }
    }

    private static func clamped(_ value: Double, _ lower: Double, _ upper: Double) -> Double {
        min(upper, max(lower, value))
    }
}

public struct DebugWorkbenchPlotSnapshot: Codable, Equatable {
    public var id: String
    public var title: String
    public var layerCount: Int
    public var pointCount: Int
    public var pointCloudCount: Int
    public var sourceSampleCount: UInt64
    public var displaySampleCount: Int
    public var boundedLayerCount: Int
    public var payloadStrategies: [String]
    public var rasterLayerCount: Int
    public var pointRasterLayerCount: Int
    public var intervalLayerCount: Int
    public var panelCount: Int
    public var overlayShapeCount: Int
    public var annotationCount: Int
    public var styleRevision: UInt64
    public var dataFingerprint: String

    public init(plot: WorkbenchPlotDocument) {
        id = plot.id
        title = plot.title
        let layers = plot.allLayers
        layerCount = layers.count
        pointCount = layers.reduce(0) { total, layer in total + layer.points.count }
        pointCloudCount = layers.reduce(0) { total, layer in total + (layer.pointCloud?.count ?? 0) }
        sourceSampleCount = layers.reduce(0) { total, layer in total + layer.dataProfile.sourceSampleCount }
        displaySampleCount = layers.reduce(0) { total, layer in total + layer.dataProfile.displaySampleCount }
        boundedLayerCount = layers.filter(\.dataProfile.isDisplayPayloadBounded).count
        payloadStrategies = layers.map(\.dataProfile.strategy.rawValue)
        rasterLayerCount = layers.filter { $0.kind == .raster }.count
        pointRasterLayerCount = layers.filter { $0.pointRaster != nil }.count
        intervalLayerCount = layers.filter { $0.kind == .interval }.count
        panelCount = plot.panels.count
        overlayShapeCount = plot.allOverlayShapes.count
        annotationCount = plot.allAnnotations.count
        styleRevision = plot.styleRevision
        dataFingerprint = plot.dataFingerprint
    }
}

public enum WorkbenchPlotSamples {
    public static func all() -> [WorkbenchPlotDocument] {
        [
            plotmsLikeVisibility(),
            uvCoverage(),
            millionPointPixels(),
            continuousPointPixels(),
            antennaLayout(),
            metadataIntervals(),
            stackedAmplitudePhase(),
            profileSpectrum(),
            imageDisplay()
        ]
    }

    private static let cachedMillionPointPixels = makeMillionPointPixels()
    private static let cachedContinuousPointPixels = makeContinuousPointPixels()

    public static func plotmsLikeVisibility() -> WorkbenchPlotDocument {
        let axes = [
            WorkbenchPlotAxis(
                id: "channel",
                label: "Channel",
                unit: "",
                range: WorkbenchPlotRange(lower: 0, upper: 127)
            ),
            WorkbenchPlotAxis(
                id: "amplitude",
                label: "Amplitude",
                unit: "Jy",
                range: WorkbenchPlotRange(lower: 0, upper: 5.5)
            )
        ]
        let target = visibilityPoints(
            field: "IRC+10216",
            spectralWindow: "spw 1",
            correlation: "RR",
            rowOffset: 0,
            phase: 0.0,
            amplitudeScale: 1.0
        )
        let calibrator = visibilityPoints(
            field: "J0954+1743",
            spectralWindow: "spw 1",
            correlation: "LL",
            rowOffset: 512,
            phase: 0.8,
            amplitudeScale: 0.72
        )
        let fit = stride(from: 0, through: 127, by: 4).map { channel in
            let center = 63.0
            let sigma = 17.0
            let x = Double(channel)
            let y = 0.65 + 3.4 * exp(-pow(x - center, 2) / (2 * sigma * sigma))
            return WorkbenchPlotPoint(
                x: x,
                y: y,
                provenance: WorkbenchPlotPointProvenance(source: "display-only gaussian overlay")
            )
        }

        return WorkbenchPlotDocument(
            id: "sample-plotms-visibility",
            title: "plotms-like Visibility",
            subtitle: "Amplitude by channel with two field/correlation series and a fit overlay",
            axes: axes,
            layers: [
                WorkbenchPlotLayer(
                    id: "target-rr",
                    title: "IRC+10216 RR",
                    kind: .scatter,
                    xAxisID: "channel",
                    yAxisID: "amplitude",
                    points: target,
                    style: WorkbenchPlotLayerStyle(colorHex: "#2563eb", symbolSize: 3.8, opacity: 0.82),
                    provenanceSummary: "Display-ready points from a MeasurementSet-style payload with row provenance.",
                    dataProfile: WorkbenchPlotLayerDataProfile(
                        sourceSampleCount: UInt64(target.count),
                        displaySampleCount: target.count,
                        pointBudget: 8_000,
                        strategy: .inlineDisplayPoints,
                        sourceDescription: "Visibility samples selected from MS rows, channels, and correlations.",
                        provenanceKey: "ms-row-channel-correlation"
                    )
                ),
                WorkbenchPlotLayer(
                    id: "phasecal-ll",
                    title: "J0954+1743 LL",
                    kind: .scatter,
                    xAxisID: "channel",
                    yAxisID: "amplitude",
                    points: calibrator,
                    style: WorkbenchPlotLayerStyle(colorHex: "#dc2626", symbolSize: 3.2, opacity: 0.62),
                    provenanceSummary: "Second field/correlation series for plot widget styling and legend behavior.",
                    dataProfile: WorkbenchPlotLayerDataProfile(
                        sourceSampleCount: UInt64(calibrator.count),
                        displaySampleCount: calibrator.count,
                        pointBudget: 8_000,
                        strategy: .inlineDisplayPoints,
                        sourceDescription: "Second visibility selection represented as display points.",
                        provenanceKey: "ms-row-channel-correlation"
                    )
                ),
                WorkbenchPlotLayer(
                    id: "gaussian-fit",
                    title: "Gaussian fit overlay",
                    kind: .line,
                    xAxisID: "channel",
                    yAxisID: "amplitude",
                    points: fit,
                    style: WorkbenchPlotLayerStyle(colorHex: "#111827", symbolSize: 0, lineWidth: 2.2, opacity: 0.9),
                    provenanceSummary: "Future Python layer: fitted curve over plotted points.",
                    dataProfile: WorkbenchPlotLayerDataProfile(
                        sourceSampleCount: UInt64(fit.count),
                        displaySampleCount: fit.count,
                        pointBudget: 4_000,
                        strategy: .inlineDisplayPoints,
                        sourceDescription: "Small analytical overlay generated from displayed or selected points.",
                        provenanceKey: "fit-overlay"
                    )
                )
            ],
            annotations: [
                WorkbenchPlotAnnotation(id: "line-center", x: 63, y: 4.1, text: "line center")
            ]
        )
    }

    public static func uvCoverage() -> WorkbenchPlotDocument {
        let axes = [
            WorkbenchPlotAxis(
                id: "u",
                label: "u",
                unit: "klambda",
                range: WorkbenchPlotRange(lower: -140, upper: 140)
            ),
            WorkbenchPlotAxis(
                id: "v",
                label: "v",
                unit: "klambda",
                range: WorkbenchPlotRange(lower: -140, upper: 140)
            )
        ]
        let tracks = (0..<6).map { track in
            let points = (0..<180).flatMap { sample -> [WorkbenchPlotPoint] in
                let angle = (Double(sample) / 179.0) * Double.pi * 1.35 + Double(track) * 0.23
                let radius = 28.0 + Double(track) * 17.0 + 5.0 * sin(Double(sample) * 0.09)
                let u = radius * cos(angle)
                let v = radius * sin(angle) * (0.55 + Double(track) * 0.06)
                let point = WorkbenchPlotPoint(
                    x: u,
                    y: v,
                    provenance: WorkbenchPlotPointProvenance(
                        row: UInt64(track * 1_000 + sample),
                        field: "IRC+10216",
                        spectralWindow: "spw \(track % 3)",
                        source: "uv track"
                    )
                )
                return [
                    point,
                    WorkbenchPlotPoint(
                        x: -u,
                        y: -v,
                        provenance: point.provenance
                    )
                ]
            }
            return WorkbenchPlotLayer(
                id: "track-\(track)",
                title: "Track \(track + 1)",
                kind: .scatter,
                xAxisID: "u",
                yAxisID: "v",
                points: points,
                style: WorkbenchPlotLayerStyle(
                    colorHex: ["#2563eb", "#16a34a", "#dc2626", "#9333ea", "#ea580c", "#0891b2"][track],
                    symbolSize: 2.2,
                    opacity: 0.72
                ),
                provenanceSummary: "Mirrored uv points with row provenance.",
                dataProfile: WorkbenchPlotLayerDataProfile(
                    sourceSampleCount: UInt64(points.count),
                    displaySampleCount: points.count,
                    pointBudget: 12_000,
                    strategy: .inlineDisplayPoints,
                    sourceDescription: "Displayed uv track points with mirrored baselines.",
                    provenanceKey: "ms-row-uvw"
                )
            )
        }
        return WorkbenchPlotDocument(
            id: "sample-uv-coverage",
            title: "UV Coverage",
            subtitle: "Dense mirrored uv scatter with multiple tracks",
            axes: axes,
            layers: tracks,
            annotations: [
                WorkbenchPlotAnnotation(id: "origin", x: 0, y: 0, text: "array center")
            ]
        )
    }

    public static func millionPointPixels() -> WorkbenchPlotDocument {
        cachedMillionPointPixels
    }

    private static func makeMillionPointPixels() -> WorkbenchPlotDocument {
        let axes = [
            WorkbenchPlotAxis(
                id: "channel",
                label: "Channel",
                unit: "",
                range: WorkbenchPlotRange(lower: 0, upper: 1023)
            ),
            WorkbenchPlotAxis(
                id: "amplitude",
                label: "Amplitude",
                unit: "Jy",
                range: WorkbenchPlotRange(lower: 0, upper: 6.2)
            )
        ]
        let sampleCount = 2_000_000
        let channelCount = 1_024
        var xValues: [Double] = []
        var yValues: [Double] = []
        xValues.reserveCapacity(sampleCount)
        yValues.reserveCapacity(sampleCount)

        for index in 0..<sampleCount {
            let channel = Double(index % channelCount)
            let sweep = Double(index / channelCount)
            let center = 520.0 + 42.0 * sin(sweep * 0.018)
            let sigma = 82.0 + 12.0 * cos(sweep * 0.009)
            let normalizedChannel = (channel - center) / sigma
            let line = 3.1 * exp(-0.5 * normalizedChannel * normalizedChannel)
            let bandpass = 0.72 + 0.12 * sin(channel * 0.019) + 0.08 * cos(channel * 0.043)
            let baseline = 0.00042 * channel
            let fieldOffset = 0.18 * Double((index / (channelCount * 13)) % 5)
            let deterministicNoise = 0.18 * sin(Double(index) * 0.011) + 0.11 * cos(Double(index) * 0.023)
            xValues.append(channel)
            yValues.append(max(0.02, bandpass + baseline + fieldOffset + line + deterministicNoise))
        }

        let pointCloud = WorkbenchPlotPointCloud(
            xValues: xValues,
            yValues: yValues,
            provenanceSamples: [
                WorkbenchPlotPointProvenance(
                    row: 0,
                    field: "IRC+10216",
                    spectralWindow: "spw 1",
                    correlation: "RR",
                    source: "million-point sample start"
                ),
                WorkbenchPlotPointProvenance(
                    row: UInt64(sampleCount - 1),
                    field: "IRC+10216",
                    spectralWindow: "spw 1",
                    correlation: "RR",
                    source: "million-point sample end"
                )
            ]
        )
        let pointRaster = WorkbenchPlotPointRaster.build(
            from: pointCloud,
            xRange: axes[0].range,
            yRange: axes[1].range,
            width: 512,
            height: 256,
            xFootprintDataWidth: 1.0
        )

        return WorkbenchPlotDocument(
            id: "sample-million-point-pixels",
            title: "Two Million Channel Bins",
            subtitle: "2M channelized samples rendered with channel-bin footprints",
            axes: axes,
            layers: [
                WorkbenchPlotLayer(
                    id: "visibility-pixels",
                    title: "IRC+10216 RR pixels",
                    kind: .scatter,
                    xAxisID: "channel",
                    yAxisID: "amplitude",
                    pointCloud: pointCloud,
                    pointRaster: pointRaster,
                    style: WorkbenchPlotLayerStyle(colorHex: "#0f766e", symbolSize: 1.0, opacity: 0.85),
                    provenanceSummary: "Columnar point cloud retained for extraction/fitting; point raster drives rendering.",
                    dataProfile: WorkbenchPlotLayerDataProfile(
                        sourceSampleCount: UInt64(pointCloud.count),
                        displaySampleCount: pointRaster.occupiedPixelCount,
                        pointBudget: pointRaster.width * pointRaster.height,
                        strategy: .channelBinPointRaster,
                        sourceDescription: "Two million channel/amplitude samples held by the Swift plot widget.",
                        provenanceKey: "ms-row-channel-correlation",
                        xBinWidth: 1.0
                    )
                )
            ],
            annotations: [
                WorkbenchPlotAnnotation(id: "pixel-line-center", x: 520, y: 4.45, text: "line ridge")
            ]
        )
    }

    public static func continuousPointPixels() -> WorkbenchPlotDocument {
        cachedContinuousPointPixels
    }

    private static func makeContinuousPointPixels() -> WorkbenchPlotDocument {
        let axes = [
            WorkbenchPlotAxis(
                id: "x",
                label: "X",
                unit: "",
                range: WorkbenchPlotRange(lower: -9, upper: 9)
            ),
            WorkbenchPlotAxis(
                id: "y",
                label: "Y",
                unit: "",
                range: WorkbenchPlotRange(lower: -5, upper: 7)
            )
        ]
        let sampleCount = 2_000_000
        let centers: [(x: Double, y: Double)] = [(-3.8, 1.2), (1.0, 3.3), (4.6, 0.4), (0.0, 1.0)]
        let widths: [(x: Double, y: Double)] = [(2.1, 1.25), (2.8, 1.55), (1.65, 1.15), (8.6, 5.3)]
        var xValues: [Double] = []
        var yValues: [Double] = []
        xValues.reserveCapacity(sampleCount)
        yValues.reserveCapacity(sampleCount)

        for index in 0..<sampleCount {
            let component = index % centers.count
            let u = unitHash(UInt64(index), seed: 0x9e37_79b9_7f4a_7c15)
            let v = unitHash(UInt64(index), seed: 0xbf58_476d_1ce4_e5b9)
            let w = unitHash(UInt64(index), seed: 0x94d0_49bb_1331_11eb)
            let center = centers[component]
            let width = widths[component]
            let x = center.x + width.x * (u * 2 - 1) + 0.18 * sin(Double(index) * 0.00037)
            let y = center.y + width.y * (v * 2 - 1) + 0.28 * sin(x * 1.55) + 0.14 * cos(w * Double.pi * 2)
            xValues.append(x)
            yValues.append(y)
        }

        let pointCloud = WorkbenchPlotPointCloud(
            xValues: xValues,
            yValues: yValues,
            provenanceSamples: [
                WorkbenchPlotPointProvenance(source: "continuous scatter sample start"),
                WorkbenchPlotPointProvenance(
                    row: UInt64(sampleCount - 1),
                    source: "continuous scatter sample end"
                )
            ]
        )
        let pointRaster = WorkbenchPlotPointRaster.build(
            from: pointCloud,
            xRange: axes[0].range,
            yRange: axes[1].range,
            width: 512,
            height: 256
        )

        return WorkbenchPlotDocument(
            id: "sample-continuous-point-pixels",
            title: "Two Million Continuous Points",
            subtitle: "True scatter cloud rasterized to occupied display pixels",
            axes: axes,
            layers: [
                WorkbenchPlotLayer(
                    id: "continuous-pixels",
                    title: "continuous scatter pixels",
                    kind: .scatter,
                    xAxisID: "x",
                    yAxisID: "y",
                    pointCloud: pointCloud,
                    pointRaster: pointRaster,
                    style: WorkbenchPlotLayerStyle(colorHex: "#2563eb", symbolSize: 1.0, opacity: 0.78),
                    provenanceSummary: "Columnar continuous point cloud retained for extraction/fitting; point raster drives rendering.",
                    dataProfile: WorkbenchPlotLayerDataProfile(
                        sourceSampleCount: UInt64(pointCloud.count),
                        displaySampleCount: pointRaster.occupiedPixelCount,
                        pointBudget: pointRaster.width * pointRaster.height,
                        strategy: .singlePixelPointRaster,
                        sourceDescription: "Two million continuous x/y samples held by the Swift plot widget.",
                        provenanceKey: "continuous-x-y"
                    )
                )
            ],
            annotations: [
                WorkbenchPlotAnnotation(id: "continuous-clump", x: 1.0, y: 3.3, text: "continuous cloud")
            ]
        )
    }

    private static func unitHash(_ value: UInt64, seed: UInt64) -> Double {
        var z = value &+ seed
        z = (z ^ (z >> 30)) &* 0xbf58_476d_1ce4_e5b9
        z = (z ^ (z >> 27)) &* 0x94d0_49bb_1331_11eb
        z = z ^ (z >> 31)
        return Double(z >> 11) / 9_007_199_254_740_992.0
    }

    public static func imageDisplay() -> WorkbenchPlotDocument {
        let width = 48
        let height = 48
        var values: [Double] = []
        values.reserveCapacity(width * height)
        for y in 0..<height {
            for x in 0..<width {
                let dx = (Double(x) - 24.0) / 8.4
                let dy = (Double(y) - 25.0) / 6.8
                let source = 3.6 * exp(-(dx * dx + dy * dy) / 2.0)
                let ridge = 0.8 * exp(-pow((Double(y) - 15.0) / 4.5, 2)) * sin(Double(x) * 0.22)
                let ripple = 0.18 * sin(Double(x) * 0.7) * cos(Double(y) * 0.39)
                values.append(source + ridge + ripple)
            }
        }
        let raster = WorkbenchPlotRaster(
            width: width,
            height: height,
            values: values,
            valueRange: WorkbenchPlotRange(
                lower: values.min() ?? 0,
                upper: values.max() ?? 1
            ),
            stretch: .percentile,
            colorMap: .viridis,
            xAxisLabel: "Right Ascension offset",
            yAxisLabel: "Declination offset",
            colorbarLabel: "Jy/beam"
        )
        let axes = [
            WorkbenchPlotAxis(
                id: "ra",
                label: "RA offset",
                unit: "arcsec",
                range: WorkbenchPlotRange(lower: -12, upper: 12)
            ),
            WorkbenchPlotAxis(
                id: "dec",
                label: "Dec offset",
                unit: "arcsec",
                range: WorkbenchPlotRange(lower: -12, upper: 12)
            )
        ]
        return WorkbenchPlotDocument(
            id: "sample-image-display",
            title: "Image Display",
            subtitle: "Raster plane with Astropy-inspired stretch and WCS-like labels",
            axes: axes,
            layers: [
                WorkbenchPlotLayer(
                    id: "image-plane",
                    title: "Channel 32",
                    kind: .raster,
                    xAxisID: "ra",
                    yAxisID: "dec",
                    raster: raster,
                    style: WorkbenchPlotLayerStyle(colorHex: "#ffffff", opacity: 1),
                    provenanceSummary: "Display-ready CASA image plane sample with WCS-like axes.",
                    dataProfile: WorkbenchPlotLayerDataProfile(
                        sourceSampleCount: 2048 * 2048,
                        displaySampleCount: values.count,
                        pointBudget: 256 * 256,
                        strategy: .rasterOverview,
                        sourceDescription: "Image plane overview resampled from a larger source plane.",
                        provenanceKey: "image-plane-wcs"
                    )
                )
            ],
            annotations: [
                WorkbenchPlotAnnotation(id: "beam", x: -9.5, y: -9.8, text: "beam"),
                WorkbenchPlotAnnotation(id: "peak", x: 0.2, y: 0.4, text: "peak"),
                WorkbenchPlotAnnotation(id: "pinned-probe", x: 4.7, y: -2.8, text: "probe")
            ],
            overlayShapes: [
                WorkbenchPlotOverlayShape(
                    id: "region-source-core",
                    points: [
                        WorkbenchPlotPoint(x: -2.2, y: -1.3),
                        WorkbenchPlotPoint(x: 2.4, y: -1.0),
                        WorkbenchPlotPoint(x: 3.0, y: 2.1),
                        WorkbenchPlotPoint(x: -1.8, y: 2.6)
                    ],
                    label: "region",
                    style: WorkbenchPlotLayerStyle(colorHex: "#f59e0b", lineWidth: 2.0, opacity: 0.85)
                ),
                WorkbenchPlotOverlayShape(
                    id: "profile-cut",
                    points: [
                        WorkbenchPlotPoint(x: -8.5, y: -5.5),
                        WorkbenchPlotPoint(x: 8.0, y: 5.8)
                    ],
                    closed: false,
                    label: "profile cut",
                    style: WorkbenchPlotLayerStyle(colorHex: "#111827", lineWidth: 1.8, opacity: 0.9)
                )
            ]
        )
    }

    public static func antennaLayout() -> WorkbenchPlotDocument {
        let axes = [
            WorkbenchPlotAxis(
                id: "east",
                label: "East offset",
                unit: "m",
                range: WorkbenchPlotRange(lower: -95, upper: 95)
            ),
            WorkbenchPlotAxis(
                id: "north",
                label: "North offset",
                unit: "m",
                range: WorkbenchPlotRange(lower: -75, upper: 90)
            )
        ]
        let antennas = [
            ("ea01", -72.0, -21.0, 7.5),
            ("ea02", -44.0, -48.0, 5.0),
            ("ea03", -18.0, -11.0, 4.5),
            ("ea04", 0.0, 0.0, 9.0),
            ("ea05", 25.0, 18.0, 6.0),
            ("ea06", 52.0, 43.0, 5.5),
            ("ea07", 74.0, 70.0, 4.5),
            ("ea08", 38.0, -37.0, 6.8)
        ].map { antenna in
            WorkbenchPlotPoint(
                x: antenna.1,
                y: antenna.2,
                label: antenna.0,
                symbolSize: antenna.3,
                provenance: WorkbenchPlotPointProvenance(source: "antenna \(antenna.0)")
            )
        }
        return WorkbenchPlotDocument(
            id: "sample-antenna-layout",
            title: "Antenna Layout",
            subtitle: "Per-point labels and marker sizes for listobs-style array geometry",
            axes: axes,
            layers: [
                WorkbenchPlotLayer(
                    id: "antennas",
                    title: "VLA antennas",
                    kind: .scatter,
                    xAxisID: "east",
                    yAxisID: "north",
                    points: antennas,
                    style: WorkbenchPlotLayerStyle(colorHex: "#7c3aed", symbolSize: 5.0, opacity: 0.9),
                    provenanceSummary: "Antenna table positions with labels retained as point metadata.",
                    dataProfile: WorkbenchPlotLayerDataProfile(
                        sourceSampleCount: UInt64(antennas.count),
                        displaySampleCount: antennas.count,
                        pointBudget: 2_000,
                        strategy: .inlineDisplayPoints,
                        sourceDescription: "ANTENNA subtable positions mapped to labeled plot points.",
                        provenanceKey: "antenna-position"
                    )
                )
            ],
            annotations: [
                WorkbenchPlotAnnotation(id: "array-center", x: 0, y: 0, text: "reference")
            ]
        )
    }

    public static func metadataIntervals() -> WorkbenchPlotDocument {
        let laneLabels = ["scan 1", "scan 2", "spw 0", "spw 1", "spw 2"]
        let axes = [
            WorkbenchPlotAxis(
                id: "time",
                label: "Time",
                unit: "min",
                range: WorkbenchPlotRange(lower: 0, upper: 72)
            ),
            WorkbenchPlotAxis(
                id: "lane",
                label: "Metadata lane",
                unit: "",
                range: WorkbenchPlotRange(lower: -0.5, upper: Double(laneLabels.count) - 0.5),
                laneLabels: laneLabels
            )
        ]
        let scans = [
            WorkbenchPlotInterval(id: "scan-1a", xStart: 2, xEnd: 18, y: 0, label: "target"),
            WorkbenchPlotInterval(id: "scan-1b", xStart: 25, xEnd: 42, y: 0, label: "target"),
            WorkbenchPlotInterval(id: "scan-2a", xStart: 9, xEnd: 22, y: 1, label: "phasecal"),
            WorkbenchPlotInterval(id: "scan-2b", xStart: 48, xEnd: 66, y: 1, label: "bandpass")
        ]
        let spws = [
            WorkbenchPlotInterval(id: "spw-0", xStart: 4, xEnd: 30, y: 2, label: "1.420 GHz"),
            WorkbenchPlotInterval(id: "spw-1", xStart: 17, xEnd: 55, y: 3, label: "1.421 GHz"),
            WorkbenchPlotInterval(id: "spw-2", xStart: 36, xEnd: 70, y: 4, label: "continuum")
        ]
        return WorkbenchPlotDocument(
            id: "sample-metadata-intervals",
            title: "Scan and SPW Coverage",
            subtitle: "Interval bars on categorical lanes for listobs timeline surfaces",
            axes: axes,
            layers: [
                WorkbenchPlotLayer(
                    id: "scan-bars",
                    title: "Scans",
                    kind: .interval,
                    xAxisID: "time",
                    yAxisID: "lane",
                    intervals: scans,
                    style: WorkbenchPlotLayerStyle(colorHex: "#2563eb", opacity: 0.78),
                    provenanceSummary: "Scan timeline intervals.",
                    dataProfile: WorkbenchPlotLayerDataProfile(
                        sourceSampleCount: UInt64(scans.count),
                        displaySampleCount: scans.count,
                        strategy: .inlineDisplayPoints,
                        sourceDescription: "Listobs scan intervals represented as bars.",
                        provenanceKey: "scan-interval"
                    )
                ),
                WorkbenchPlotLayer(
                    id: "spw-bars",
                    title: "Spectral windows",
                    kind: .interval,
                    xAxisID: "time",
                    yAxisID: "lane",
                    intervals: spws,
                    style: WorkbenchPlotLayerStyle(colorHex: "#16a34a", opacity: 0.70),
                    provenanceSummary: "Spectral-window coverage intervals.",
                    dataProfile: WorkbenchPlotLayerDataProfile(
                        sourceSampleCount: UInt64(spws.count),
                        displaySampleCount: spws.count,
                        strategy: .inlineDisplayPoints,
                        sourceDescription: "Listobs spectral-window coverage represented as bars.",
                        provenanceKey: "spw-coverage"
                    )
                )
            ]
        )
    }

    public static func stackedAmplitudePhase() -> WorkbenchPlotDocument {
        let timeAxis = WorkbenchPlotAxis(
            id: "time",
            label: "Time",
            unit: "min",
            range: WorkbenchPlotRange(lower: 0, upper: 60)
        )
        let ampAxis = WorkbenchPlotAxis(
            id: "amp",
            label: "Amplitude",
            unit: "Jy",
            range: WorkbenchPlotRange(lower: 0, upper: 5.5)
        )
        let phaseAxis = WorkbenchPlotAxis(
            id: "phase",
            label: "Phase",
            unit: "deg",
            range: WorkbenchPlotRange(lower: -180, upper: 180),
            drawsOnTrailingEdge: true
        )
        let amp = (0..<120).map { index in
            let t = Double(index) * 0.5
            return WorkbenchPlotPoint(x: t, y: 2.2 + 0.7 * sin(t * 0.18) + 0.18 * cos(t * 0.73))
        }
        let phase = (0..<120).map { index in
            let t = Double(index) * 0.5
            return WorkbenchPlotPoint(x: t, y: 95 * sin(t * 0.12) + 35 * cos(t * 0.31))
        }
        let residual = (0..<120).map { index in
            let t = Double(index) * 0.5
            return WorkbenchPlotPoint(x: t, y: 0.35 + 0.25 * abs(sin(t * 0.22)))
        }
        let ampPhasePanel = WorkbenchPlotPanel(
            id: "amp-phase-panel",
            title: "Amplitude and phase",
            axes: [timeAxis, ampAxis, phaseAxis],
            layers: [
                WorkbenchPlotLayer(
                    id: "amp-time",
                    title: "Amplitude",
                    kind: .scatter,
                    xAxisID: "time",
                    yAxisID: "amp",
                    points: amp,
                    style: WorkbenchPlotLayerStyle(colorHex: "#2563eb", symbolSize: 2.6, opacity: 0.8),
                    provenanceSummary: "ScatterGrid amplitude panel.",
                    dataProfile: WorkbenchPlotLayerDataProfile(
                        sourceSampleCount: UInt64(amp.count),
                        displaySampleCount: amp.count,
                        pointBudget: 10_000,
                        strategy: .inlineDisplayPoints,
                        sourceDescription: "Amplitude samples in a faceted scatter page.",
                        provenanceKey: "scatter-grid-amplitude"
                    )
                ),
                WorkbenchPlotLayer(
                    id: "phase-time",
                    title: "Phase",
                    kind: .line,
                    xAxisID: "time",
                    yAxisID: "phase",
                    points: phase,
                    style: WorkbenchPlotLayerStyle(colorHex: "#dc2626", lineWidth: 1.6, opacity: 0.82),
                    provenanceSummary: "Secondary-axis phase overlay.",
                    dataProfile: WorkbenchPlotLayerDataProfile(
                        sourceSampleCount: UInt64(phase.count),
                        displaySampleCount: phase.count,
                        pointBudget: 10_000,
                        strategy: .inlineDisplayPoints,
                        sourceDescription: "Phase samples using a trailing y axis.",
                        provenanceKey: "scatter-grid-phase"
                    )
                )
            ]
        )
        let residualPanel = WorkbenchPlotPanel(
            id: "residual-panel",
            title: "Residual",
            axes: [
                timeAxis,
                WorkbenchPlotAxis(
                    id: "residual",
                    label: "Residual",
                    unit: "Jy",
                    range: WorkbenchPlotRange(lower: 0, upper: 1.0)
                )
            ],
            layers: [
                WorkbenchPlotLayer(
                    id: "residual-time",
                    title: "Residual",
                    kind: .line,
                    xAxisID: "time",
                    yAxisID: "residual",
                    points: residual,
                    style: WorkbenchPlotLayerStyle(colorHex: "#0f766e", lineWidth: 1.8, opacity: 0.86),
                    provenanceSummary: "Stacked plot page residual line.",
                    dataProfile: WorkbenchPlotLayerDataProfile(
                        sourceSampleCount: UInt64(residual.count),
                        displaySampleCount: residual.count,
                        pointBudget: 10_000,
                        strategy: .inlineDisplayPoints,
                        sourceDescription: "Second panel in a stacked scatter page.",
                        provenanceKey: "scatter-page-residual"
                    )
                )
            ]
        )
        return WorkbenchPlotDocument(
            id: "sample-stacked-amp-phase",
            title: "Stacked Visibility Page",
            subtitle: "Multi-panel scatter page with a secondary phase axis",
            axes: [],
            layers: [],
            panels: [ampPhasePanel, residualPanel]
        )
    }

    public static func profileSpectrum() -> WorkbenchPlotDocument {
        let axes = [
            WorkbenchPlotAxis(
                id: "velocity",
                label: "Velocity",
                unit: "km/s",
                range: WorkbenchPlotRange(lower: -42, upper: 42)
            ),
            WorkbenchPlotAxis(
                id: "intensity",
                label: "Intensity",
                unit: "Jy/beam",
                range: WorkbenchPlotRange(lower: -0.2, upper: 4.4)
            )
        ]
        let profile = stride(from: -42.0, through: 42.0, by: 1.0).map { velocity in
            let gap = velocity > -6 && velocity < 3
            let selected = abs(velocity - 18) < 0.1
            let line = 3.2 * exp(-pow((velocity - 16) / 9.5, 2) / 2)
            let shoulder = 1.1 * exp(-pow((velocity + 24) / 7.0, 2) / 2)
            return WorkbenchPlotPoint(
                x: velocity,
                y: 0.25 + line + shoulder + 0.08 * sin(velocity),
                lineBreakBefore: gap || abs(velocity - 3) < 0.1,
                selected: selected,
                provenance: WorkbenchPlotPointProvenance(source: selected ? "selected profile channel" : "profile channel")
            )
        }
        let overlay = stride(from: -42.0, through: 42.0, by: 2.0).map { velocity in
            WorkbenchPlotPoint(
                x: velocity,
                y: 0.22 + 2.9 * exp(-pow((velocity - 15) / 11.0, 2) / 2)
            )
        }
        return WorkbenchPlotDocument(
            id: "sample-profile-spectrum",
            title: "Image Profile Spectrum",
            subtitle: "Masked line gaps, selected channel marker, and overlay profile",
            axes: axes,
            layers: [
                WorkbenchPlotLayer(
                    id: "masked-profile",
                    title: "Profile",
                    kind: .line,
                    xAxisID: "velocity",
                    yAxisID: "intensity",
                    points: profile,
                    style: WorkbenchPlotLayerStyle(colorHex: "#2563eb", lineWidth: 2.0, opacity: 0.9),
                    provenanceSummary: "Image profile with masked channels and selected sample.",
                    dataProfile: WorkbenchPlotLayerDataProfile(
                        sourceSampleCount: UInt64(profile.count),
                        displaySampleCount: profile.count,
                        pointBudget: 6_000,
                        strategy: .inlineDisplayPoints,
                        sourceDescription: "imexplore profile samples with mask-gap metadata.",
                        provenanceKey: "image-profile"
                    )
                ),
                WorkbenchPlotLayer(
                    id: "overlay-profile",
                    title: "Overlay profile",
                    kind: .line,
                    xAxisID: "velocity",
                    yAxisID: "intensity",
                    points: overlay,
                    style: WorkbenchPlotLayerStyle(colorHex: "#f59e0b", lineWidth: 1.5, opacity: 0.75),
                    provenanceSummary: "Comparison or fitted profile overlay.",
                    dataProfile: WorkbenchPlotLayerDataProfile(
                        sourceSampleCount: UInt64(overlay.count),
                        displaySampleCount: overlay.count,
                        pointBudget: 6_000,
                        strategy: .inlineDisplayPoints,
                        sourceDescription: "Optional imexplore profile overlay.",
                        provenanceKey: "image-profile-overlay"
                    )
                )
            ],
            annotations: [
                WorkbenchPlotAnnotation(id: "selected-channel", x: 18, y: 3.45, text: "selected")
            ]
        )
    }

    private static func visibilityPoints(
        field: String,
        spectralWindow: String,
        correlation: String,
        rowOffset: Int,
        phase: Double,
        amplitudeScale: Double
    ) -> [WorkbenchPlotPoint] {
        (0..<128).map { channel in
            let x = Double(channel)
            let center = 63.0 + 2.0 * sin(phase)
            let sigma = 18.0
            let line = 3.2 * exp(-pow(x - center, 2) / (2 * sigma * sigma))
            let ripple = 0.26 * sin(x * 0.21 + phase) + 0.14 * cos(x * 0.53 + phase)
            let baseline = 0.58 + 0.004 * x
            return WorkbenchPlotPoint(
                x: x,
                y: max(0.05, amplitudeScale * (baseline + line + ripple)),
                provenance: WorkbenchPlotPointProvenance(
                    row: UInt64(rowOffset + channel),
                    field: field,
                    spectralWindow: spectralWindow,
                    correlation: correlation,
                    source: "plotms-like sample"
                )
            )
        }
    }
}
