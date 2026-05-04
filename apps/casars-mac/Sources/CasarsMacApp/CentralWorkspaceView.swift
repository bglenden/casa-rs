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
                Button("Calibrate Task") {
                    store.openDefaultTab(kind: .task)
                }
                .disabled(!store.state.isDemoProject)
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
        if dataset.kind == .measurementSet {
            MeasurementSetPlotPanel(store: store, dataset: dataset)
        } else {
            HStack(alignment: .top, spacing: 16) {
                SummaryBox(
                    title: "Overview",
                    values: [
                        dataset.size,
                        "Bytes: \(byteCount(dataset.sizeBytes))",
                        "Shape: \(formatShape(dataset.shape))"
                    ]
                )
                SummaryBox(title: "Fields", values: dataset.fields)
                SummaryBox(title: "Spectral Windows", values: dataset.spectralWindows)
            }

            SummaryBox(
                title: "Explorer Status",
                values: ["A real summary is available. A specialized \(dataset.kind.explorerName) is not implemented yet."]
            )

            SummaryBox(title: "Probe Notes", values: [dataset.notes] + dataset.diagnostics)
        }
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
                store.runMeasurementSetPlot(datasetID: dataset.id)
            } label: {
                Label(plotState.status == .running ? "Generating" : "Generate", systemImage: "play.fill")
            }
            .disabled(plotState.status == .running)
            .accessibilityIdentifier("msPlot.generate.\(dataset.id)")

            Button {
                showingAdvancedSetup.toggle()
            } label: {
                Label("Advanced", systemImage: "slider.horizontal.3")
            }
            .popover(isPresented: $showingAdvancedSetup, arrowEdge: .top) {
                advancedPlotSetup
                    .frame(width: 320)
            }
            .accessibilityIdentifier("msPlot.advanced.\(dataset.id)")
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 9)
        .background(.regularMaterial)
        .clipShape(RoundedRectangle(cornerRadius: 10))
        .shadow(color: Color.black.opacity(0.16), radius: 10, x: 0, y: 4)
    }

    private var advancedPlotSetup: some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("Plot Filters")
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
        ZStack(alignment: .bottomLeading) {
            plotImage
                .frame(maxWidth: .infinity, maxHeight: .infinity)

            VStack(alignment: .leading, spacing: 12) {
                Text(dataset.name)
                    .workbenchFont(.caption, weight: .semibold)
                Text(plotState.result?.selectionSummary ?? "Select a plot and generate it")
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
            }
            .padding(.horizontal, 14)
            .padding(.vertical, 10)
            .background(.regularMaterial)
            .clipShape(RoundedRectangle(cornerRadius: 8))
            .padding(14)
        }
    }

    @ViewBuilder
    private var plotMetadata: some View {
        if let result = plotState.result {
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
    private var plotImage: some View {
        if let result = plotState.result, let image = NSImage(data: result.imageBytes) {
            VStack(alignment: .leading, spacing: 8) {
                Text(result.title)
                    .workbenchFont(.subheadline, weight: .semibold)
                Image(nsImage: image)
                    .resizable()
                    .scaledToFit()
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                    .background(Color(nsColor: .windowBackgroundColor))
                    .clipShape(RoundedRectangle(cornerRadius: 6))
                Text(result.summary)
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(2)
            }
            .padding(16)
            .frame(maxWidth: .infinity, alignment: .topLeading)
            .accessibilityIdentifier("msPlot.image.\(dataset.id)")
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
