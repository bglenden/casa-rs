import Foundation

public enum WorkbenchPlotLayerKind: String, Codable, Equatable {
    case scatter
    case line
    case raster
}

public enum WorkbenchPlotPayloadStrategy: String, Codable, Equatable {
    case inlineDisplayPoints
    case viewportLevelOfDetail
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

    public init(
        sourceSampleCount: UInt64,
        displaySampleCount: Int,
        pointBudget: Int = 50_000,
        strategy: WorkbenchPlotPayloadStrategy,
        sourceDescription: String,
        provenanceKey: String? = nil
    ) {
        self.sourceSampleCount = sourceSampleCount
        self.displaySampleCount = displaySampleCount
        self.pointBudget = pointBudget
        self.strategy = strategy
        self.sourceDescription = sourceDescription
        self.provenanceKey = provenanceKey
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

public struct WorkbenchPlotAxis: Identifiable, Codable, Equatable {
    public let id: String
    public var label: String
    public var unit: String
    public var range: WorkbenchPlotRange
    public var labelsVisible: Bool
    public var gridVisible: Bool

    public init(
        id: String,
        label: String,
        unit: String,
        range: WorkbenchPlotRange,
        labelsVisible: Bool = true,
        gridVisible: Bool = true
    ) {
        self.id = id
        self.label = label
        self.unit = unit
        self.range = range
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
    public var provenance: WorkbenchPlotPointProvenance?

    public init(x: Double, y: Double, provenance: WorkbenchPlotPointProvenance? = nil) {
        self.x = x
        self.y = y
        self.provenance = provenance
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

public struct WorkbenchPlotLayer: Identifiable, Codable, Equatable {
    public let id: String
    public var title: String
    public var kind: WorkbenchPlotLayerKind
    public var xAxisID: String
    public var yAxisID: String
    public var points: [WorkbenchPlotPoint]
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
        self.raster = raster
        self.style = style
        self.provenanceSummary = provenanceSummary
        self.dataProfile = dataProfile ?? WorkbenchPlotLayerDataProfile(
            sourceSampleCount: UInt64(max(points.count, raster?.values.count ?? 0)),
            displaySampleCount: max(points.count, raster?.values.count ?? 0),
            pointBudget: kind == .line ? 100_000 : 50_000,
            strategy: kind == .raster ? .rasterOverview : .inlineDisplayPoints,
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
    public var showLegend: Bool
    public var styleRevision: UInt64

    public init(
        id: String,
        title: String,
        subtitle: String,
        axes: [WorkbenchPlotAxis],
        layers: [WorkbenchPlotLayer],
        annotations: [WorkbenchPlotAnnotation] = [],
        showLegend: Bool = true,
        styleRevision: UInt64 = 0
    ) {
        self.id = id
        self.title = title
        self.subtitle = subtitle
        self.axes = axes
        self.layers = layers
        self.annotations = annotations
        self.showLegend = showLegend
        self.styleRevision = styleRevision
    }

    public var dataFingerprint: String {
        var parts = [
            id,
            axes.map { "\($0.id):\($0.range.lower.bitPattern):\($0.range.upper.bitPattern)" }
                .joined(separator: ",")
        ]
        for layer in layers {
            parts.append("\(layer.id):\(layer.kind.rawValue):\(layer.points.count)")
            parts.append(
                "profile:\(layer.dataProfile.sourceSampleCount):\(layer.dataProfile.displaySampleCount):\(layer.dataProfile.strategy.rawValue)"
            )
            if let first = layer.points.first {
                parts.append("first:\(first.x.bitPattern):\(first.y.bitPattern)")
            }
            if let last = layer.points.last {
                parts.append("last:\(last.x.bitPattern):\(last.y.bitPattern)")
            }
            if let raster = layer.raster {
                parts.append(
                    "raster:\(raster.width)x\(raster.height):\(raster.values.count):\(raster.valueRange.lower.bitPattern):\(raster.valueRange.upper.bitPattern)"
                )
            }
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
        guard let index = layers.firstIndex(where: { $0.id == layerID }) else { return }
        update(&layers[index])
        styleRevision += 1
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
    public var sourceSampleCount: UInt64
    public var displaySampleCount: Int
    public var boundedLayerCount: Int
    public var payloadStrategies: [String]
    public var rasterLayerCount: Int
    public var annotationCount: Int
    public var styleRevision: UInt64
    public var dataFingerprint: String

    public init(plot: WorkbenchPlotDocument) {
        id = plot.id
        title = plot.title
        layerCount = plot.layers.count
        pointCount = plot.layers.reduce(0) { total, layer in total + layer.points.count }
        sourceSampleCount = plot.layers.reduce(0) { total, layer in total + layer.dataProfile.sourceSampleCount }
        displaySampleCount = plot.layers.reduce(0) { total, layer in total + layer.dataProfile.displaySampleCount }
        boundedLayerCount = plot.layers.filter(\.dataProfile.isDisplayPayloadBounded).count
        payloadStrategies = plot.layers.map(\.dataProfile.strategy.rawValue)
        rasterLayerCount = plot.layers.filter { $0.kind == .raster }.count
        annotationCount = plot.annotations.count
        styleRevision = plot.styleRevision
        dataFingerprint = plot.dataFingerprint
    }
}

public enum WorkbenchPlotSamples {
    public static func all() -> [WorkbenchPlotDocument] {
        [
            plotmsLikeVisibility(),
            uvCoverage(),
            imageDisplay()
        ]
    }

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
                        sourceSampleCount: 1_800_000,
                        displaySampleCount: target.count,
                        pointBudget: 8_000,
                        strategy: .viewportLevelOfDetail,
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
                        sourceSampleCount: 1_800_000,
                        displaySampleCount: calibrator.count,
                        pointBudget: 8_000,
                        strategy: .viewportLevelOfDetail,
                        sourceDescription: "Second visibility selection represented as a viewport-level display payload.",
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
                    sourceSampleCount: 4_000_000,
                    displaySampleCount: points.count,
                    pointBudget: 12_000,
                    strategy: .viewportLevelOfDetail,
                    sourceDescription: "Large uv track reduced to display points for the current viewport.",
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
                WorkbenchPlotAnnotation(id: "peak", x: 0.2, y: 0.4, text: "peak")
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
