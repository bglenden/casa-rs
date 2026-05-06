import CasarsMacCore
import AppKit
import Foundation
import SwiftUI

struct CentralWorkspaceView: View {
    @ObservedObject var store: WorkbenchStore

    var body: some View {
        VStack(spacing: 0) {
            tabStrip

            Divider()

            activePanel
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        }
        .background(Color(nsColor: .textBackgroundColor))
    }

    private var tabStrip: some View {
        HStack(spacing: 4) {
            ForEach(store.state.tabs) { tab in
                let isActive = tab.id == store.state.activeTabID
                HStack(spacing: 4) {
                    Button {
                        store.activateTab(tab.id)
                    } label: {
                        HStack(spacing: 6) {
                            Image(systemName: icon(for: tab.kind))
                            Text(tab.title)
                                .lineLimit(1)
                        }
                    }
                    .buttonStyle(.borderless)

                    Button {
                        store.closeTab(tab.id)
                    } label: {
                        Image(systemName: "xmark")
                            .workbenchFont(.caption2, weight: .semibold)
                            .foregroundStyle(.secondary)
                            .frame(width: 16, height: 16)
                    }
                    .buttonStyle(.borderless)
                    .help("Close \(tab.title)")
                    .accessibilityLabel("Close \(tab.title)")
                    .accessibilityIdentifier("central.tab.close.\(tab.id)")
                }
                .padding(.leading, 10)
                .padding(.trailing, 6)
                .padding(.vertical, 7)
                .background(isActive ? Color.accentColor.opacity(0.14) : Color.clear)
                .clipShape(RoundedRectangle(cornerRadius: 6))
                .accessibilityIdentifier("central.tab.\(tab.id)")
            }

            Menu {
                Button("Dataset Explorer") {
                    store.openDefaultTab(kind: .datasetExplorer)
                }
                .disabled(store.state.selectedDataset == nil)
                Button(store.state.isDemoProject ? "Calibrate Task" : "Dirty Imaging Task") {
                    store.openDefaultTab(kind: .task)
                }
                .disabled(store.state.selectedDataset == nil)
                Button("Plot Samples") {
                    store.openDefaultTab(kind: .plotSamples)
                }
                Button("AI Chat") {
                    store.openDefaultTab(kind: .aiChat)
                }
                .disabled(!store.state.isDemoProject)
                Button("Python") {
                    store.openDefaultTab(kind: .python)
                }
                .disabled(!store.state.isDemoProject)
                Button("History") {
                    store.openDefaultTab(kind: .history)
                }
            } label: {
                Image(systemName: "plus")
                    .frame(width: 28, height: 28)
            }
            .buttonStyle(.borderless)
            .help("Open central work tab")
            .accessibilityIdentifier("central.tab.plus")

            Spacer()
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 7)
        .background(.bar)
    }

    @ViewBuilder
    private var activePanel: some View {
        if let tab = store.state.tabs.first(where: { $0.id == store.state.activeTabID }) {
            switch tab.kind {
            case .datasetExplorer:
                DatasetExplorerPanel(store: store, datasetID: tab.datasetID)
            case .task:
                TaskPanel(store: store)
            case .plotSamples:
                PlotSamplesPanel(store: store)
            case .aiChat:
                AIChatPanel(store: store)
            case .python:
                PythonPanel(store: store)
            case .history:
                HistoryPanel(store: store)
            }
        } else {
            EmptyWorkbenchPanel(store: store)
        }
    }

    private func icon(for kind: WorkbenchTabKind) -> String {
        switch kind {
        case .datasetExplorer: "chart.xyaxis.line"
        case .task: "slider.horizontal.3"
        case .plotSamples: "chart.xyaxis.line"
        case .aiChat: "sparkles"
        case .python: "terminal"
        case .history: "clock"
        }
    }
}

struct EmptyWorkbenchPanel: View {
    @ObservedObject var store: WorkbenchStore

    var body: some View {
        VStack(alignment: .leading, spacing: 18) {
            PanelHeader(
                title: store.state.hasProject ? "No active tab" : "Open a casa-rs project",
                subtitle: store.state.hasProject
                    ? "Select a dataset or open a work tab."
                    : "Choose a directory and casa-rs will probe it for supported datasets."
            )

            HStack(spacing: 12) {
                Button {
                    if let url = ProjectOpenPanel.chooseDirectory() {
                        store.openProject(path: url.path)
                    }
                } label: {
                    Label("Open Project Directory", systemImage: "folder")
                }
                .accessibilityIdentifier("empty.openProject")

                Button {
                    store.openFixtureProject()
                } label: {
                    Label("Open Demo Project", systemImage: "shippingbox")
                }
                .accessibilityIdentifier("empty.openDemoProject")
            }

            if !store.state.lastErrors.isEmpty {
                SummaryBox(title: "Recent Errors", values: store.state.lastErrors)
            }
        }
        .frame(maxWidth: 560, alignment: .leading)
        .padding(28)
        .accessibilityIdentifier("panel.emptyWorkbench")
    }
}

struct DatasetExplorerPanel: View {
    @ObservedObject var store: WorkbenchStore
    let datasetID: String?

    var body: some View {
        Group {
            if let dataset {
                if dataset.kind == .measurementSet && !store.state.isDemoProject {
                    MeasurementSetPlotPanel(store: store, dataset: dataset)
                } else {
                    ScrollView {
                        VStack(alignment: .leading, spacing: 18) {
                            PanelHeader(title: dataset.kind.explorerName, subtitle: explorerSubtitle(for: dataset))

                            if store.state.isDemoProject {
                                demoExplorerContent(for: dataset)
                            } else {
                                realExplorerContent(for: dataset)
                            }

                            Text(dataset.path)
                                .workbenchFont(.caption, design: .monospaced)
                                .foregroundStyle(.secondary)
                        }
                        .padding(20)
                    }
                }
            } else {
                VStack(alignment: .leading, spacing: 18) {
                    PanelHeader(title: "Dataset Explorer", subtitle: "Select a dataset before opening an explorer")
                }
                .padding(20)
            }
        }
        .accessibilityIdentifier("panel.datasetExplorer")
    }

    private var dataset: DatasetSummary? {
        if let datasetID {
            return store.state.project.datasets.first { $0.id == datasetID }
        }
        return store.state.selectedDataset
    }

    private func explorerSubtitle(for dataset: DatasetSummary) -> String {
        if store.state.isDemoProject {
            "\(dataset.name) - \(dataset.size) - \(dataset.units)"
        } else {
            "\(dataset.name) - \(dataset.size) - \(byteCount(dataset.sizeBytes))"
        }
    }

    @ViewBuilder
    private func demoExplorerContent(for dataset: DatasetSummary) -> some View {
        HStack(alignment: .top, spacing: 16) {
            SummaryBox(title: primarySummaryTitle(for: dataset), values: primarySummaryValues(for: dataset))
            SummaryBox(title: secondarySummaryTitle(for: dataset), values: secondarySummaryValues(for: dataset))
            SummaryBox(title: "Demo Notes", values: [dataset.notes])
        }

        HStack(spacing: 16) {
            ForEach(plotPlaceholders(for: dataset)) { plot in
                PlotPlaceholder(title: plot.title, caption: plot.caption)
            }
        }
    }

    @ViewBuilder
    private func realExplorerContent(for dataset: DatasetSummary) -> some View {
        switch dataset.kind {
        case .measurementSet:
            MeasurementSetPlotPanel(store: store, dataset: dataset)
        case .imageCube:
            imageExplorerContent(for: dataset)
        case .calibrationTable, .table:
            tableExplorerContent(for: dataset)
        case .runProduct:
            productExplorerContent(for: dataset)
        }
    }

    @ViewBuilder
    private func imageExplorerContent(for dataset: DatasetSummary) -> some View {
        let explorerState = store.state.imageExplorers[dataset.id]
        VStack(alignment: .leading, spacing: 18) {
            HStack {
                Picker("View", selection: Binding(
                    get: { explorerState?.selectedView ?? "plane" },
                    set: { store.setImageExplorerView($0, datasetID: dataset.id) }
                )) {
                    Text("Plane").tag("plane")
                    Text("Spectrum").tag("spectrum")
                    Text("Metadata").tag("metadata")
                    Text("Coordinates").tag("coordinates")
                }
                .pickerStyle(.segmented)
                .frame(width: 360)
                .accessibilityIdentifier("imageExplorer.view.\(dataset.id)")

                Button {
                    store.refreshImageExplorer(datasetID: dataset.id)
                } label: {
                    Label("Refresh", systemImage: "arrow.clockwise")
                }
                .accessibilityIdentifier("imageExplorer.refresh.\(dataset.id)")

                if let snapshot = explorerState?.snapshot {
                    Text(snapshot.statusLine)
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                } else if let error = explorerState?.lastError {
                    Text(error)
                        .workbenchFont(.caption)
                        .foregroundStyle(.red)
                        .lineLimit(1)
                }

                Spacer()
            }

            HStack(alignment: .top, spacing: 16) {
                SummaryBox(
                    title: "Image",
                    values: [
                        dataset.size,
                        "Units: \(dataset.units.isEmpty ? "Unknown" : dataset.units)",
                        "Bytes: \(byteCount(dataset.sizeBytes))",
                        "Shape: \(formatShape(dataset.shape))"
                    ]
                )
                SummaryBox(title: "WCS and Beam", values: dataset.diagnostics.filter(isImageGeometryDiagnostic))
                SummaryBox(title: "Masks and Regions", values: imageMaskRegionValues(for: dataset))
            }

            if let snapshot = explorerState?.snapshot {
                ImageExplorerSnapshotView(snapshot: snapshot)
            } else {
                ImagePreviewPlaceholder(dataset: dataset)
            }

            SummaryBox(title: "Probe Notes", values: [dataset.notes] + dataset.diagnostics)
        }
    }

    @ViewBuilder
    private func tableExplorerContent(for dataset: DatasetSummary) -> some View {
        let browserState = store.state.tableBrowsers[dataset.id]
        VStack(alignment: .leading, spacing: 18) {
            HStack {
                Picker("View", selection: Binding(
                    get: { browserState?.selectedView ?? "overview" },
                    set: { store.setTableBrowserView($0, datasetID: dataset.id) }
                )) {
                    Text("Overview").tag("overview")
                    Text("Columns").tag("columns")
                    Text("Keywords").tag("keywords")
                    Text("Cells").tag("cells")
                    Text("Subtables").tag("subtables")
                }
                .pickerStyle(.segmented)
                .frame(width: 460)
                .accessibilityIdentifier("tableBrowser.view.\(dataset.id)")

                Button {
                    store.refreshTableBrowser(datasetID: dataset.id)
                } label: {
                    Label("Refresh", systemImage: "arrow.clockwise")
                }
                .accessibilityIdentifier("tableBrowser.refresh.\(dataset.id)")

                if let snapshot = browserState?.snapshot {
                    Text(snapshot.statusLine)
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                } else if let error = browserState?.lastError {
                    Text(error)
                        .workbenchFont(.caption)
                        .foregroundStyle(.red)
                        .lineLimit(1)
                }

                Spacer()
            }

            HStack(alignment: .top, spacing: 16) {
                SummaryBox(
                    title: "Table",
                    values: [
                        dataset.size,
                        "Type: \(dataset.units.isEmpty ? "casacore table" : dataset.units)",
                        "Rows: \(dataset.shape.first.map(String.init) ?? "Unknown")",
                        "Bytes: \(byteCount(dataset.sizeBytes))"
                    ]
                )
                SummaryBox(title: "Columns", values: dataset.columns)
                SummaryBox(title: "Subtables", values: dataset.subtables)
            }

            if let snapshot = browserState?.snapshot {
                TableBrowserSnapshotView(snapshot: snapshot)
            } else {
                TablePreviewSummary(dataset: dataset)
            }
            SummaryBox(title: "Probe Notes", values: [dataset.notes] + dataset.diagnostics)
        }
    }

    private func productExplorerContent(for dataset: DatasetSummary) -> some View {
        VStack(alignment: .leading, spacing: 18) {
            SummaryBox(
                title: "Product",
                values: [
                    dataset.size,
                    dataset.units,
                    dataset.path
                ]
            )
            SummaryBox(title: "Product Metadata", values: [dataset.notes] + dataset.diagnostics)
        }
    }

    private func isImageGeometryDiagnostic(_ value: String) -> Bool {
        value.hasPrefix("Cell size:")
            || value.hasPrefix("Center:")
            || value.hasPrefix("Cube center frequency:")
            || value.hasPrefix("Total bandwidth:")
            || value.hasPrefix("Channel separation:")
            || value.hasPrefix("Beam:")
            || value.hasPrefix("Median beam:")
    }

    private func imageMaskRegionValues(for dataset: DatasetSummary) -> [String] {
        let values = dataset.diagnostics.filter {
            $0.hasPrefix("Default mask:") || $0.lowercased().contains("mask") || $0.lowercased().contains("region")
        }
        return values.isEmpty ? ["No mask or region metadata reported"] : values
    }

    private func primarySummaryTitle(for dataset: DatasetSummary) -> String {
        switch dataset.kind {
        case .measurementSet, .runProduct:
            "Fields"
        case .imageCube:
            "Axes and planes"
        case .calibrationTable, .table:
            "Rows and columns"
        }
    }

    private func primarySummaryValues(for dataset: DatasetSummary) -> [String] {
        switch dataset.kind {
        case .measurementSet, .runProduct:
            dataset.fields
        case .imageCube:
            [dataset.size, dataset.units] + dataset.spectralWindows
        case .calibrationTable, .table:
            [dataset.size, dataset.units]
        }
    }

    private func secondarySummaryTitle(for dataset: DatasetSummary) -> String {
        switch dataset.kind {
        case .measurementSet, .imageCube, .runProduct:
            "Spectral windows"
        case .calibrationTable:
            "Solutions"
        case .table:
            "Metadata"
        }
    }

    private func secondarySummaryValues(for dataset: DatasetSummary) -> [String] {
        switch dataset.kind {
        case .measurementSet, .imageCube, .runProduct:
            dataset.spectralWindows
        case .calibrationTable:
            dataset.fields + dataset.spectralWindows + dataset.scans
        case .table:
            [dataset.kind.rawValue, dataset.path]
        }
    }

    private func plotPlaceholders(for dataset: DatasetSummary) -> [ExplorerPlot] {
        switch dataset.kind {
        case .measurementSet:
            [
                ExplorerPlot(title: "Amplitude vs. channel", caption: "selected target field"),
                ExplorerPlot(title: "UV distance", caption: "visibility sample")
            ]
        case .imageCube:
            [
                ExplorerPlot(title: "Cube movie", caption: "channel planes"),
                ExplorerPlot(title: "Spectrum", caption: "cursor sample")
            ]
        case .calibrationTable:
            [
                ExplorerPlot(title: "Gain amplitude", caption: "solution intervals"),
                ExplorerPlot(title: "Gain phase", caption: "antenna overlay")
            ]
        case .table:
            [
                ExplorerPlot(title: "Table preview", caption: "schema and rows"),
                ExplorerPlot(title: "Column statistics", caption: "numeric columns")
            ]
        case .runProduct:
            [
                ExplorerPlot(title: "Product summary", caption: "derived dataset"),
                ExplorerPlot(title: "History links", caption: "upstream task lineage")
            ]
        }
    }
}

private func byteCount(_ bytes: UInt64) -> String {
    ByteCountFormatter.string(fromByteCount: Int64(bytes), countStyle: .file)
}

private func formatShape(_ shape: [UInt64]) -> String {
    shape.isEmpty ? "None" : shape.map(String.init).joined(separator: " x ")
}

private struct ExplorerPlot: Identifiable {
    let title: String
    let caption: String

    var id: String { title }
}

private struct ImageExplorerSnapshotView: View {
    let snapshot: ImageExplorerSnapshot

    var body: some View {
        Grid(alignment: .topLeading, horizontalSpacing: 14, verticalSpacing: 14) {
            GridRow {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Plane")
                        .workbenchFont(.headline)
                    if let plane = snapshot.plane {
                        ImagePlaneRasterView(plane: plane)
                            .frame(minHeight: 280)
                        Text("\(plane.width)x\(plane.height), clip \(plane.clipMin.formatted())...\(plane.clipMax.formatted()) \(plane.valueUnit)")
                            .workbenchFont(.caption)
                            .foregroundStyle(.secondary)
                    } else {
                        Text("No renderable plane in current image browser snapshot.")
                            .foregroundStyle(.secondary)
                    }
                }

                VStack(alignment: .leading, spacing: 10) {
                    SummaryBox(
                        title: "Session",
                        values: [
                            "View: \(snapshot.activeView)",
                            "Shape: \(formatShape(snapshot.shape.map(UInt64.init)))",
                            "Plane: \(snapshot.capabilities.renderablePlane ? "renderable" : "unavailable")",
                            "World coordinates: \(snapshot.capabilities.worldCoordsAvailable ? "available" : "pixel-only")"
                        ]
                    )
                    SummaryBox(title: "Inspector", values: snapshot.inspectorLines)
                }
            }

            GridRow {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Profile")
                        .workbenchFont(.headline)
                    if let profile = snapshot.profile {
                        WorkbenchPlotView(plot: profilePlotDocument(profile))
                            .frame(minHeight: 220)
                        Text("\(profile.samples.count) samples on \(profile.axisName)")
                            .workbenchFont(.caption)
                            .foregroundStyle(.secondary)
                    } else {
                        Text("No profile for the current cursor/plane.")
                            .foregroundStyle(.secondary)
                    }
                }

                SummaryBox(
                    title: "Masks and Regions",
                    values: maskRegionValues(snapshot: snapshot)
                )
            }
        }
        .accessibilityIdentifier("imageExplorer.snapshot")
    }

    private func maskRegionValues(snapshot: ImageExplorerSnapshot) -> [String] {
        var values: [String] = []
        values.append("Masks: \(snapshot.maskNames.isEmpty ? "none" : snapshot.maskNames.joined(separator: ", "))")
        values.append("Saved regions: \(snapshot.savedRegionNames.isEmpty ? "none" : snapshot.savedRegionNames.joined(separator: ", "))")
        if let region = snapshot.region {
            values.append("\(region.label): \(region.shapeCount) shape(s), \(region.closedShapeCount) closed")
            values.append(region.editing ? "Region edit active" : "Region edit inactive")
        }
        return values
    }

    private func profilePlotDocument(_ profile: ImageExplorerSnapshot.Profile) -> WorkbenchPlotDocument {
        let points = profile.samples
            .filter(\.finite)
            .map { WorkbenchPlotPoint(x: Double($0.sampleIndex), y: $0.value) }
        let layer = WorkbenchPlotLayer(
            id: "profile-\(profile.axis)",
            title: "\(profile.axisName) profile",
            kind: .line,
            xAxisID: "sample",
            yAxisID: "value",
            points: points,
            style: WorkbenchPlotLayerStyle(colorHex: "#4F7DFF", symbolSize: 2.0, lineWidth: 1.4),
            provenanceSummary: "Profile samples from Rust imexplore session snapshot."
        )
        let xRange = WorkbenchPlotRange(
            lower: points.map(\.x).min() ?? 0,
            upper: points.map(\.x).max() ?? 1
        )
        let yRange = WorkbenchPlotRange(
            lower: points.map(\.y).min() ?? 0,
            upper: points.map(\.y).max() ?? 1
        )
        return WorkbenchPlotDocument(
            id: "image-profile-\(profile.axis)",
            title: "\(profile.axisName) Profile",
            subtitle: "\(profile.valueUnit) vs \(profile.axisUnit)",
            axes: [
                WorkbenchPlotAxis(id: "sample", label: profile.axisName, unit: profile.axisUnit, range: expandedRange(xRange)),
                WorkbenchPlotAxis(id: "value", label: "Value", unit: profile.valueUnit, range: expandedRange(yRange))
            ],
            layers: [layer]
        )
    }

    private func expandedRange(_ range: WorkbenchPlotRange) -> WorkbenchPlotRange {
        guard range.lower == range.upper else {
            return range
        }
        return WorkbenchPlotRange(lower: range.lower - 0.5, upper: range.upper + 0.5)
    }
}

private struct ImagePlaneRasterView: View {
    let plane: ImageExplorerSnapshot.Plane
    @State private var image: NSImage?

    var body: some View {
        Group {
            if let image {
                Image(nsImage: image)
                    .resizable()
                    .interpolation(.none)
                    .scaledToFit()
            } else {
                Color(nsColor: .windowBackgroundColor)
            }
        }
        .background(Color(nsColor: .windowBackgroundColor))
        .clipShape(RoundedRectangle(cornerRadius: 6))
        .overlay(RoundedRectangle(cornerRadius: 6).stroke(Color.secondary.opacity(0.18)))
        .onAppear(perform: updateImage)
        .onChange(of: plane.pixelsU8) { _ in updateImage() }
    }

    private func updateImage() {
        guard plane.width > 0, plane.height > 0, plane.pixelsU8.count == plane.width * plane.height else {
            image = nil
            return
        }
        let bitmap = NSBitmapImageRep(
            bitmapDataPlanes: nil,
            pixelsWide: plane.width,
            pixelsHigh: plane.height,
            bitsPerSample: 8,
            samplesPerPixel: 4,
            hasAlpha: true,
            isPlanar: false,
            colorSpaceName: .deviceRGB,
            bytesPerRow: plane.width * 4,
            bitsPerPixel: 32
        )
        guard let data = bitmap?.bitmapData else {
            image = nil
            return
        }
        for index in 0..<(plane.width * plane.height) {
            let value = plane.pixelsU8[index]
            let offset = index * 4
            data[offset] = value
            data[offset + 1] = value
            data[offset + 2] = value
            data[offset + 3] = 255
        }
        let output = NSImage(size: NSSize(width: plane.width, height: plane.height))
        if let bitmap {
            output.addRepresentation(bitmap)
        }
        image = output
    }
}

private struct TableBrowserSnapshotView: View {
    let snapshot: TableBrowserSnapshot

    var body: some View {
        Grid(alignment: .topLeading, horizontalSpacing: 14, verticalSpacing: 14) {
            GridRow {
                SummaryBox(
                    title: "Browser",
                    values: [
                        "View: \(snapshot.view)",
                        "Focus: \(snapshot.focus)",
                        "Breadcrumb: \(snapshot.breadcrumb.map(\.label).joined(separator: " / "))"
                    ]
                )
                SummaryBox(title: "Inspector", values: inspectorValues)
            }

            GridRow {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Content")
                        .workbenchFont(.headline)
                    VStack(alignment: .leading, spacing: 2) {
                        ForEach(snapshot.contentLines.prefix(32), id: \.self) { line in
                            Text(line)
                                .workbenchFont(.caption, design: .monospaced)
                                .lineLimit(1)
                        }
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding(12)
                    .background(Color(nsColor: .windowBackgroundColor))
                    .clipShape(RoundedRectangle(cornerRadius: 6))
                    .overlay(RoundedRectangle(cornerRadius: 6).stroke(Color.secondary.opacity(0.18)))
                }
                .gridCellColumns(2)
            }
        }
        .accessibilityIdentifier("tableBrowser.snapshot")
    }

    private var inspectorValues: [String] {
        guard let inspector = snapshot.inspector else {
            return ["No selected inspector payload"]
        }
        return [inspector.title] + inspector.renderedLines
    }
}

struct MeasurementSetPlotPanel: View {
    @ObservedObject var store: WorkbenchStore
    let dataset: DatasetSummary
    @State private var showingAdvancedSetup = false

    var body: some View {
        ZStack(alignment: .top) {
            plotSurface
            plotCommandBar
                .padding(.top, 14)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .accessibilityIdentifier("msPlot.panel.\(dataset.id)")
    }

    private var plotState: MeasurementSetExplorerPlotState {
        store.state.measurementSetPlots[dataset.id] ?? MeasurementSetExplorerPlotState.defaultState(for: dataset)
    }

    private var visiblePlotResult: MeasurementSetPlotResultSummary? {
        guard let result = plotState.result, result.matches(plotState: plotState) else {
            return nil
        }
        return result
    }

    private var plotCommandBar: some View {
        HStack(spacing: 10) {
            Picker("Plot", selection: Binding(
                get: { plotState.preset },
                set: { store.setMeasurementSetPlotPreset($0, datasetID: dataset.id) }
            )) {
                ForEach(MeasurementSetExplorerPlotPreset.allCases) { preset in
                    Text(preset.title).tag(preset)
                }
            }
            .labelsHidden()
            .frame(width: 220)
            .accessibilityIdentifier("msPlot.preset.\(dataset.id)")

            Button {
                showingAdvancedSetup.toggle()
            } label: {
                Label("Selections", systemImage: "slider.horizontal.3")
            }
            .popover(isPresented: $showingAdvancedSetup, arrowEdge: .top) {
                plotSelections
                    .frame(width: 320)
            }
            .accessibilityIdentifier("msPlot.selections.\(dataset.id)")

            Button {
                store.runMeasurementSetPlot(datasetID: dataset.id)
            } label: {
                Label(plotState.status == .running ? "Generating" : "Generate", systemImage: "play.fill")
            }
            .disabled(plotState.status == .running)
            .accessibilityIdentifier("msPlot.generate.\(dataset.id)")
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 9)
        .background(.regularMaterial)
        .clipShape(RoundedRectangle(cornerRadius: 10))
        .shadow(color: Color.black.opacity(0.16), radius: 10, x: 0, y: 4)
    }

    private var plotSelections: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("Selections")
                .workbenchFont(.headline)

            Picker("Field", selection: Binding(
                get: { plotState.selectedField ?? "all" },
                set: { store.setMeasurementSetPlotField($0, datasetID: dataset.id) }
            )) {
                Text("all").tag("all")
                ForEach(dataset.fields, id: \.self) { field in
                    Text(field).tag(field)
                }
            }
            .accessibilityIdentifier("msPlot.field.\(dataset.id)")

            Picker("Spectral window", selection: Binding(
                get: { plotState.selectedSpectralWindow ?? "all" },
                set: { store.setMeasurementSetPlotSpectralWindow($0, datasetID: dataset.id) }
            )) {
                Text("all").tag("all")
                ForEach(dataset.spectralWindows, id: \.self) { spectralWindow in
                    Text(spectralWindow).tag(spectralWindow)
                }
            }
            .accessibilityIdentifier("msPlot.spw.\(dataset.id)")

            Picker("Correlation", selection: Binding(
                get: { plotState.selectedCorrelation ?? "all" },
                set: { store.setMeasurementSetPlotCorrelation($0, datasetID: dataset.id) }
            )) {
                Text("all").tag("all")
                ForEach(dataset.correlations, id: \.self) { correlation in
                    Text(correlation).tag(correlation)
                }
            }
            .accessibilityIdentifier("msPlot.correlation.\(dataset.id)")

            Picker("Data column", selection: Binding(
                get: { plotState.dataColumn },
                set: { store.setMeasurementSetPlotDataColumn($0, datasetID: dataset.id) }
            )) {
                ForEach(dataset.dataColumns.isEmpty ? ["DATA"] : dataset.dataColumns, id: \.self) { column in
                    Text(column).tag(column)
                }
            }
            .disabled(plotState.preset == .uvCoverage)
            .accessibilityIdentifier("msPlot.dataColumn.\(dataset.id)")

            Divider()

            plotMetadata
        }
        .padding(16)
    }

    private var plotSurface: some View {
        plotDocumentSurface
            .frame(maxWidth: .infinity, maxHeight: .infinity)
    }

    @ViewBuilder
    private var plotMetadata: some View {
        if let result = visiblePlotResult {
            VStack(alignment: .leading, spacing: 5) {
                Text(result.presetLabel)
                    .workbenchFont(.subheadline, weight: .semibold)
                Text("\(result.xAxis.label) -> \(result.yAxis.label)")
                Text("\(result.renderedPointCount) points, \(result.series.count) series")
                Text(result.renderer)
                ForEach(result.diagnostics, id: \.self) { diagnostic in
                    Text(diagnostic)
                        .foregroundStyle(.orange)
                }
            }
            .workbenchFont(.caption)
            .foregroundStyle(.secondary)
        } else if let error = plotState.lastError {
            Text(error)
                .workbenchFont(.caption)
                .foregroundStyle(.red)
        } else {
            Text("No plot rendered yet.")
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
        }
    }

    @ViewBuilder
    private var plotDocumentSurface: some View {
        if let result = visiblePlotResult {
            VStack(alignment: .leading, spacing: 8) {
                Text(result.title)
                    .workbenchFont(.subheadline, weight: .semibold)
                if !result.plotDocument.headerLines.isEmpty {
                    VStack(alignment: .leading, spacing: 2) {
                        ForEach(result.plotDocument.headerLines, id: \.self) { line in
                            Text(line)
                                .workbenchFont(.caption, design: .monospaced)
                                .foregroundStyle(.secondary)
                        }
                    }
                }
                WorkbenchPlotView(plot: result.plotDocument)
                    .id(result.plotDocument.dataFingerprint)
                Text(result.summary)
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(2)
            }
            .padding(16)
            .frame(maxWidth: .infinity, alignment: .topLeading)
            .accessibilityIdentifier("msPlot.document.\(dataset.id)")
        } else {
            ZStack {
                RoundedRectangle(cornerRadius: 6)
                    .fill(Color(nsColor: .windowBackgroundColor))
                    .overlay(RoundedRectangle(cornerRadius: 6).stroke(Color.secondary.opacity(0.20)))
                VStack(spacing: 10) {
                    Image(systemName: "chart.xyaxis.line")
                        .workbenchFont(.largeTitle)
                    Text(plotState.status == .running ? "Rendering" : "Waiting for render")
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                }
            }
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .padding(16)
            .accessibilityIdentifier("msPlot.empty.\(dataset.id)")
        }
    }
}

struct TaskPanel: View {
    @ObservedObject var store: WorkbenchStore

    var body: some View {
        if store.state.isDemoProject {
            fixtureTaskBody
        } else {
            DirtyImagingTaskPanel(store: store)
        }
    }

    private var fixtureTaskBody: some View {
        VStack(spacing: 0) {
            HStack {
                VStack(alignment: .leading, spacing: 2) {
                    Text("Calibrate")
                        .workbenchFont(.title3, weight: .semibold)
                    Text("Fixture task with dataset-scoped parameters")
                        .foregroundStyle(.secondary)
                }

                Spacer()

                Button {
                    store.runTask()
                } label: {
                    Label("Run", systemImage: "play.fill")
                }
                .accessibilityIdentifier("task.run")

                Button {
                    store.stopTask()
                } label: {
                    Label("Stop", systemImage: "stop.fill")
                }
                .accessibilityIdentifier("task.stop")
            }
            .padding()
            .background(.bar)

            ScrollView {
                VStack(alignment: .leading, spacing: 18) {
                    parameterBlock
                    aiProposalBlock
                    runBlock
                }
                .padding(20)
            }
        }
        .accessibilityIdentifier("panel.task")
    }

    private var parameterBlock: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Parameters")
                .workbenchFont(.headline)
            Picker("Field", selection: Binding(
                get: { store.state.taskParameters.selectedField },
                set: { _ in }
            )) {
                ForEach(store.state.selectedDataset?.fields ?? [], id: \.self) { field in
                    Text(field).tag(field)
                }
            }
            .accessibilityIdentifier("task.parameter.field")

            Picker("Spectral window", selection: Binding(
                get: { store.state.taskParameters.selectedSpectralWindow },
                set: { store.setTaskSpectralWindow($0) }
            )) {
                ForEach(store.state.selectedDataset?.spectralWindows ?? [], id: \.self) { spw in
                    Text(spw).tag(spw)
                }
                Text("all").tag("all")
            }
            .accessibilityIdentifier("task.parameter.spw")

            LabeledContent("Output", value: store.state.taskParameters.outputName)
            Toggle("Dry run", isOn: .constant(store.state.taskParameters.dryRun))
        }
        .padding()
        .background(.regularMaterial)
        .clipShape(RoundedRectangle(cornerRadius: 8))
    }

    private var aiProposalBlock: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("AI proposal")
                .workbenchFont(.headline)

            ForEach(store.state.aiProposals) { proposal in
                VStack(alignment: .leading, spacing: 8) {
                    Text(proposal.title)
                        .workbenchFont(.subheadline, weight: .semibold)
                    Text("\(proposal.parameterName): \(proposal.oldValue) -> \(proposal.newValue)")
                        .workbenchFont(.caption)
                    Text(proposal.detail)
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)

                    HStack {
                        Button("Apply") {
                            store.applyAIProposal(proposal.id)
                        }
                        .disabled(proposal.state != .pending)
                        .accessibilityIdentifier("aiProposal.apply.\(proposal.id)")

                        Button("Reject") {
                            store.rejectAIProposal(proposal.id)
                        }
                        .disabled(proposal.state != .pending)
                        .accessibilityIdentifier("aiProposal.reject.\(proposal.id)")

                        Text(proposal.state.rawValue)
                            .workbenchFont(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
            }
        }
        .padding()
        .background(.regularMaterial)
        .clipShape(RoundedRectangle(cornerRadius: 8))
    }

    private var runBlock: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Run state")
                .workbenchFont(.headline)
            ProgressView(value: store.state.taskRun.progress)
            Text(store.state.taskRun.state.rawValue)
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
            ForEach(store.state.taskRun.logLines, id: \.self) { line in
                Text(line)
                    .workbenchFont(.caption, design: .monospaced)
            }
        }
        .padding()
        .background(.regularMaterial)
        .clipShape(RoundedRectangle(cornerRadius: 8))
    }
}

struct DirtyImagingTaskPanel: View {
    @ObservedObject var store: WorkbenchStore

    var body: some View {
        VStack(spacing: 0) {
            header

            ScrollView {
                VStack(alignment: .leading, spacing: 18) {
                    if let parameters = store.state.dirtyImagingTaskParameters {
                        selectionBlock(parameters: parameters)
                        imagingBlock(parameters: parameters)
                        outputBlock(parameters: parameters)
                        runBlock
                        runProductsBlock
                    } else {
                        PanelHeader(title: "Dirty Imaging", subtitle: "Select a MeasurementSet before opening this task")
                    }
                }
                .padding(20)
            }
        }
        .accessibilityIdentifier("panel.task.dirtyImaging")
    }

    private var header: some View {
        HStack {
            VStack(alignment: .leading, spacing: 2) {
                Text("Dirty Imaging")
                    .workbenchFont(.title3, weight: .semibold)
                Text(selectedDatasetSubtitle)
                    .foregroundStyle(.secondary)
            }

            Spacer()

            Button {
                store.runTask()
            } label: {
                Label(store.state.taskRun.state == .running ? "Running" : "Run", systemImage: "play.fill")
            }
            .disabled(store.state.taskRun.state == .running || store.state.dirtyImagingTaskParameters == nil)
            .accessibilityIdentifier("task.run")

            Button {
                store.stopTask()
            } label: {
                Label("Stop", systemImage: "stop.fill")
            }
            .disabled(store.state.taskRun.state != .running)
            .accessibilityIdentifier("task.stop")
        }
        .padding()
        .background(.bar)
    }

    private var selectedDatasetSubtitle: String {
        guard let parameters = store.state.dirtyImagingTaskParameters,
              let dataset = taskDataset(for: parameters)
        else {
            return "No MeasurementSet selected"
        }
        return "\(dataset.name) - \(dataset.size)"
    }

    private func selectionBlock(parameters: DirtyImagingTaskParameters) -> some View {
        let dataset = taskDataset(for: parameters)
        return VStack(alignment: .leading, spacing: 12) {
            Text("Selections")
                .workbenchFont(.headline)

            Picker("MeasurementSet", selection: Binding(
                get: { parameters.datasetID },
                set: { store.setDirtyImagingDataset($0) }
            )) {
                Text("Select MeasurementSet").tag("")
                ForEach(measurementSetDatasets) { dataset in
                    Text(dataset.name).tag(dataset.id)
                }
            }
            .accessibilityIdentifier("task.parameter.measurementSet")

            Picker("Source / field", selection: Binding(
                get: { parameters.selectedField ?? "all" },
                set: { store.setDirtyImagingField($0) }
            )) {
                Text("all").tag("all")
                ForEach(dataset?.fields ?? [], id: \.self) { field in
                    Text(field).tag(field)
                }
            }
            .accessibilityIdentifier("task.parameter.field")

            LabeledContent("Phase center", value: parameters.phaseCenterField ?? "auto")

            Picker("Spectral window", selection: Binding(
                get: { parameters.selectedSpectralWindow ?? "all" },
                set: { store.setDirtyImagingSpectralWindow($0) }
            )) {
                Text("all").tag("all")
                ForEach(dataset?.spectralWindows ?? [], id: \.self) { spw in
                    Text(spw).tag(spw)
                }
            }
            .accessibilityIdentifier("task.parameter.spw")

            HStack {
                TextField("Channel start", text: Binding(
                    get: { parameters.channelStart },
                    set: { store.setDirtyImagingChannelStart($0) }
                ))
                .textFieldStyle(.roundedBorder)
                .accessibilityIdentifier("task.parameter.channelStart")

                TextField("Channel count", text: Binding(
                    get: { parameters.channelCount },
                    set: { store.setDirtyImagingChannelCount($0) }
                ))
                .textFieldStyle(.roundedBorder)
                .accessibilityIdentifier("task.parameter.channelCount")
            }

            Picker("Data column", selection: Binding(
                get: { parameters.dataColumn },
                set: { store.setDirtyImagingDataColumn($0) }
            )) {
                ForEach(dataset?.dataColumns.isEmpty == false ? dataset?.dataColumns ?? [] : ["DATA"], id: \.self) { column in
                    Text(column).tag(column)
                }
            }
            .accessibilityIdentifier("task.parameter.dataColumn")

            Picker("Image plane", selection: Binding(
                get: { parameters.correlation ?? "I" },
                set: { store.setDirtyImagingCorrelation($0) }
            )) {
                Text("Stokes I").tag("I")
                ForEach(dataset?.correlations ?? [], id: \.self) { correlation in
                    Text("Raw \(correlation)").tag(correlation)
                }
            }
            .accessibilityIdentifier("task.parameter.correlation")

            Text(selectionHint(for: parameters))
                .foregroundStyle(.secondary)
                .workbenchFont(.caption)
                .accessibilityIdentifier("task.parameter.selectionHint")
        }
        .taskCard()
    }

    private var measurementSetDatasets: [DatasetSummary] {
        store.state.project.datasets.filter { $0.kind == .measurementSet }
    }

    private func taskDataset(for parameters: DirtyImagingTaskParameters) -> DatasetSummary? {
        store.state.project.datasets.first { $0.id == parameters.datasetID }
    }

    private func selectionHint(for parameters: DirtyImagingTaskParameters) -> String {
        guard let dataset = taskDataset(for: parameters) else {
            return "Pick a MeasurementSet before running dirty imaging."
        }
        if dataset.name.lowercased().contains("twhya_calibrated.ms") {
            return "Tutorial defaults pick TW Hya, spw 0, 250 px, and 0.1 arcsec cells from the ALMA First Look imaging guide."
        }
        if dataset.name == "mssel_test_small_multifield_spw.ms" {
            return "Sample defaults pick NGC4826-F3, spw 5, and raw YY: a target field with a 64-channel line window near the NGC4826 rest frequency."
        }
        return "Defaults prefer a science-like field, a multi-channel spectral window, and Stokes I when the MeasurementSet supports paired correlations."
    }

    private func imagingBlock(parameters: DirtyImagingTaskParameters) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Image")
                .workbenchFont(.headline)

            imageDimensionRow(
                label: "Image width",
                value: parameters.imageSize,
                setValue: store.setDirtyImagingImageSize,
                adjust: store.adjustDirtyImagingImageWidthToNiceSize,
                accessibilityID: "task.parameter.imageWidth"
            )

            imageDimensionRow(
                label: "Image height",
                value: parameters.imageHeight,
                setValue: store.setDirtyImagingImageHeight,
                adjust: store.adjustDirtyImagingImageHeightToNiceSize,
                accessibilityID: "task.parameter.imageHeight"
            )

            HStack(spacing: 8) {
                TextField("Cell size", text: Binding(
                    get: { String(format: "%.3f", parameters.cellArcsec) },
                    set: { value in
                        if let parsed = Double(value) {
                            store.setDirtyImagingCellArcsec(parsed)
                        }
                    }
                ))
                .textFieldStyle(.roundedBorder)
                .accessibilityIdentifier("task.parameter.cellArcsec")

                Text("arcsec")
                    .foregroundStyle(.secondary)
                    .workbenchFont(.caption)
                    .accessibilityIdentifier("task.parameter.cellArcsec.unit")
            }

            if parameters.imageSize != parameters.imageHeight {
                Label("Rectangular image sizes are not runnable yet.", systemImage: "exclamationmark.triangle")
                    .foregroundStyle(.secondary)
                    .workbenchFont(.caption)
                    .accessibilityIdentifier("task.parameter.imageShape.warning")
            }

            Picker("Weighting", selection: Binding(
                get: { parameters.weighting },
                set: { store.setDirtyImagingWeighting($0) }
            )) {
                ForEach(DirtyImagingWeighting.allCases) { weighting in
                    Text(weighting.title).tag(weighting)
                }
            }
            .accessibilityIdentifier("task.parameter.weighting")

            Toggle("Dirty only", isOn: .constant(parameters.dirtyOnly))
                .disabled(true)
                .accessibilityIdentifier("task.parameter.dirtyOnly")
        }
        .taskCard()
    }

    private func imageDimensionRow(
        label: String,
        value: Int,
        setValue: @escaping (Int) -> Void,
        adjust: @escaping () -> Void,
        accessibilityID: String
    ) -> some View {
        let assessment = DirtyImagingTaskParameters.imageDimensionAssessment(value)
        return HStack(spacing: 8) {
            Text(label)
                .frame(width: 96, alignment: .leading)

            TextField(label, text: Binding(
                get: { "\(value)" },
                set: { text in
                    if let parsed = Int(text.trimmingCharacters(in: .whitespacesAndNewlines)) {
                        setValue(parsed)
                    }
                }
            ))
            .textFieldStyle(.plain)
            .padding(.horizontal, 7)
            .padding(.vertical, 4)
            .frame(width: 82)
            .background(
                RoundedRectangle(cornerRadius: 6)
                    .fill(dimensionFill(for: assessment.severity))
            )
            .overlay(
                RoundedRectangle(cornerRadius: 6)
                    .stroke(dimensionStroke(for: assessment.severity), lineWidth: assessment.severity == .good ? 1 : 1.5)
            )
            .help(assessment.message)
            .accessibilityIdentifier(accessibilityID)

            Text("px")
                .foregroundStyle(.secondary)
                .workbenchFont(.caption)

            Button {
                adjust()
            } label: {
                Label("Adjust", systemImage: "wand.and.stars")
            }
            .disabled(!assessment.needsAdjustment)
            .help("Adjust to \(assessment.adjustedValue) px")
            .accessibilityIdentifier("\(accessibilityID).adjust")
        }
    }

    private func dimensionFill(for severity: DirtyImagingDimensionSeverity) -> Color {
        switch severity {
        case .good:
            Color(nsColor: .textBackgroundColor).opacity(0.25)
        case .warning:
            .yellow.opacity(0.40)
        case .terrible:
            .red.opacity(0.40)
        }
    }

    private func dimensionStroke(for severity: DirtyImagingDimensionSeverity) -> Color {
        switch severity {
        case .good:
            .secondary.opacity(0.35)
        case .warning:
            .yellow.opacity(0.85)
        case .terrible:
            .red.opacity(0.85)
        }
    }

    private func outputBlock(parameters: DirtyImagingTaskParameters) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Output")
                .workbenchFont(.headline)

            TextField("Image prefix", text: Binding(
                get: { parameters.outputPrefix },
                set: { store.setDirtyImagingOutputPrefix($0) }
            ))
            .textFieldStyle(.roundedBorder)
            .accessibilityIdentifier("task.parameter.outputPrefix")

            TextField("Run reason", text: Binding(
                get: { parameters.runReason },
                set: { store.setDirtyImagingRunReason($0) }
            ))
            .textFieldStyle(.roundedBorder)
            .accessibilityIdentifier("task.parameter.runReason")

            Text(parameters.requestSummary)
                .workbenchFont(.caption, design: .monospaced)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
                .accessibilityIdentifier("task.parameter.requestSummary")
        }
        .taskCard()
    }

    private var runBlock: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Run")
                .workbenchFont(.headline)
            ProgressView(value: store.state.taskRun.progress)
            Text(store.state.taskRun.state.rawValue)
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)

            valueList("Log", values: store.state.taskRun.logLines)
            valueList("Warnings", values: store.state.taskRun.warnings)
            valueList("Diagnostics", values: store.state.taskRun.diagnostics)
            valueList("Products", values: store.state.taskRun.products)
            valueList("Artifacts", values: store.state.taskRun.outputPaths)
        }
        .taskCard()
        .accessibilityIdentifier("task.runState")
    }

    @ViewBuilder
    private var runProductsBlock: some View {
        if let group = activeRunProductGroup {
            VStack(alignment: .leading, spacing: 12) {
                Text("Generated Products")
                    .workbenchFont(.headline)
                Text(group.runID)
                    .workbenchFont(.caption, design: .monospaced)
                    .foregroundStyle(.secondary)

                ForEach(group.products) { product in
                    HStack(alignment: .firstTextBaseline, spacing: 8) {
                        VStack(alignment: .leading, spacing: 3) {
                            Text(product.label)
                                .workbenchFont(.subheadline, weight: .semibold)
                            Text(product.path)
                                .workbenchFont(.caption, design: .monospaced)
                                .foregroundStyle(.secondary)
                                .lineLimit(2)
                            if product.previewPngExists, let preview = product.previewPngPath {
                                Text("Preview: \(preview)")
                                    .workbenchFont(.caption, design: .monospaced)
                                    .foregroundStyle(.secondary)
                                    .lineLimit(1)
                            }
                        }

                        Spacer()

                        Button {
                            store.openRunProduct(runID: group.runID, productID: product.id)
                        } label: {
                            Label("Open", systemImage: "arrow.up.right.square")
                        }
                        .disabled(product.datasetID == nil)
                        .accessibilityIdentifier("task.product.open.\(product.id)")
                    }
                    .padding(.vertical, 3)
                }
            }
            .taskCard()
            .accessibilityIdentifier("task.generatedProducts")
        }
    }

    private var activeRunProductGroup: RunProductGroup? {
        guard let runID = store.state.taskRun.runID else {
            return nil
        }
        return store.state.runProductGroups.first { $0.runID == runID }
    }

    private func valueList(_ title: String, values: [String]) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(title)
                .workbenchFont(.subheadline, weight: .semibold)
            if values.isEmpty {
                Text("None")
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
            } else {
                ForEach(values, id: \.self) { value in
                    Text(value)
                        .workbenchFont(.caption, design: .monospaced)
                        .foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true)
                }
            }
        }
    }
}

private extension View {
    func taskCard() -> some View {
        self
            .padding()
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(.regularMaterial)
            .clipShape(RoundedRectangle(cornerRadius: 8))
    }
}

struct AIChatPanel: View {
    @ObservedObject var store: WorkbenchStore
    @State private var draft = ""

    var body: some View {
        VStack(spacing: 0) {
            PanelHeader(title: "AI Chat", subtitle: "Fixture assistant with explicit proposals and approval")
                .padding()

            List(store.state.aiMessages) { message in
                VStack(alignment: .leading, spacing: 4) {
                    Text(message.author.rawValue)
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                    Text(message.text)
                }
            }
            .accessibilityIdentifier("aiChat.messages")

            HStack {
                TextField("Ask about the selected dataset", text: $draft)
                    .textFieldStyle(.roundedBorder)
                    .accessibilityIdentifier("aiChat.input")
                Button("Send") {
                    store.appendAIChatMessage(draft.isEmpty ? "Explain the current task proposal." : draft)
                    draft = ""
                }
            }
            .padding()
        }
        .accessibilityIdentifier("panel.aiChat")
    }
}

struct PythonPanel: View {
    @ObservedObject var store: WorkbenchStore

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            HStack {
                PanelHeader(title: "Python", subtitle: "Dual-ported fixture terminal with captured matplotlib previews")
                Spacer()
                Picker("Owner", selection: Binding(
                    get: { store.state.python.owner },
                    set: { store.setPythonOwner($0) }
                )) {
                    Text("User").tag(PythonOwner.user)
                    Text("AI").tag(PythonOwner.ai)
                }
                .pickerStyle(.segmented)
                .frame(width: 160)
                .accessibilityIdentifier("python.owner")
            }

            Text(store.state.python.owner == .ai ? "AI owns input; user entry is locked." : "User owns input.")
                .workbenchFont(.caption)
                .foregroundStyle(store.state.python.owner == .ai ? .orange : .secondary)
                .accessibilityIdentifier("python.ownershipState")

            TextEditor(text: .constant(store.state.python.buffer))
                .workbenchFont(.body, design: .monospaced)
                .disabled(store.state.python.owner == .ai)
                .frame(minHeight: 180)
                .overlay(RoundedRectangle(cornerRadius: 8).stroke(Color.secondary.opacity(0.3)))

            HStack(spacing: 16) {
                ForEach(store.state.python.capturedPlots, id: \.self) { plot in
                    PlotPlaceholder(title: plot, caption: "captured matplotlib fixture")
                }
            }

            Spacer()
        }
        .padding(20)
        .accessibilityIdentifier("panel.python")
    }
}

struct HistoryPanel: View {
    @ObservedObject var store: WorkbenchStore

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 16) {
                PanelHeader(title: "Processing History", subtitle: "Timeline of fixture actions, reasons, approvals, and affected persistent paths")

                ForEach(store.state.history) { event in
                    VStack(alignment: .leading, spacing: 6) {
                        Text(event.timestamp)
                            .workbenchFont(.caption)
                            .foregroundStyle(.secondary)
                        Text(event.title)
                            .workbenchFont(.headline)
                        Text(event.reason)
                        Text("Approval: \(event.approval)")
                            .workbenchFont(.caption)
                            .foregroundStyle(.secondary)
                        ForEach(event.affectedPaths, id: \.self) { path in
                            Text(path)
                                .workbenchFont(.caption, design: .monospaced)
                                .foregroundStyle(.secondary)
                        }
                    }
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding()
                    .background(.regularMaterial)
                    .clipShape(RoundedRectangle(cornerRadius: 8))
                    .accessibilityIdentifier("history.timeline.\(event.id)")
                }
            }
            .padding(20)
        }
        .accessibilityIdentifier("panel.history")
    }
}

struct PanelHeader: View {
    let title: String
    let subtitle: String

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(title)
                .workbenchFont(.title2, weight: .semibold)
            Text(subtitle)
                .workbenchFont(.subheadline)
                .foregroundStyle(.secondary)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}

struct SummaryBox: View {
    let title: String
    let values: [String]

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(title)
                .workbenchFont(.headline)
            ForEach(values.isEmpty ? ["None"] : values, id: \.self) { value in
                Text(value)
                    .workbenchFont(.caption)
                    .lineLimit(1)
            }
        }
        .frame(maxWidth: .infinity, minHeight: 110, alignment: .topLeading)
        .padding()
        .background(.regularMaterial)
        .clipShape(RoundedRectangle(cornerRadius: 8))
    }
}

struct ImagePreviewPlaceholder: View {
    let dataset: DatasetSummary

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Preview")
                .workbenchFont(.headline)
            ZStack {
                RoundedRectangle(cornerRadius: 6)
                    .fill(Color(nsColor: .windowBackgroundColor))
                    .overlay(RoundedRectangle(cornerRadius: 6).stroke(Color.secondary.opacity(0.20)))
                VStack(spacing: 8) {
                    Image(systemName: "photo")
                        .workbenchFont(.largeTitle)
                        .foregroundStyle(.secondary)
                    Text(previewText)
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                        .multilineTextAlignment(.center)
                }
                .padding()
            }
            .frame(minHeight: 180)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    private var previewText: String {
        if let preview = dataset.diagnostics.first(where: { $0.hasPrefix("preview:") }) {
            return preview
        }
        return "Image metadata is loaded. Raster plane preview is deferred to GUI-Wave-6."
    }
}

struct TablePreviewSummary: View {
    let dataset: DatasetSummary

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Column Preview")
                .workbenchFont(.headline)
            if dataset.columns.isEmpty {
                Text("No columns reported by the table schema.")
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
            } else {
                LazyVGrid(columns: [GridItem(.adaptive(minimum: 160), spacing: 8)], alignment: .leading, spacing: 8) {
                    ForEach(dataset.columns, id: \.self) { column in
                        Label(column, systemImage: "tablecells")
                            .lineLimit(1)
                            .padding(.horizontal, 8)
                            .padding(.vertical, 5)
                            .background(Color(nsColor: .controlBackgroundColor))
                            .clipShape(RoundedRectangle(cornerRadius: 6))
                    }
                }
            }
        }
        .padding()
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(.regularMaterial)
        .clipShape(RoundedRectangle(cornerRadius: 8))
    }
}

struct PlotPlaceholder: View {
    let title: String
    let caption: String

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(title)
                .workbenchFont(.headline)
            ZStack {
                RoundedRectangle(cornerRadius: 8)
                    .fill(Color.accentColor.opacity(0.10))
                VStack(spacing: 10) {
                    Image(systemName: "waveform.path.ecg.rectangle")
                        .workbenchFont(.largeTitle)
                    Text(caption)
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                }
            }
            .frame(height: 180)
        }
        .frame(maxWidth: .infinity)
    }
}
