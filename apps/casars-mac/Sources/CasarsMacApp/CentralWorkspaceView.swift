import CasarsMacCore
import AppKit
import Foundation
import SwiftUI
import UniformTypeIdentifiers

struct CentralWorkspaceView: View {
    @ObservedObject var store: WorkbenchStore
    var initialMeasurementSetExplorerMode: MeasurementSetExplorerMode = .summary

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
                Button("Tasks") {
                    store.openDefaultTab(kind: .task)
                }
                Button("Tutorial") {
                    store.openDefaultTab(kind: .tutorial)
                }
                .disabled(store.state.tutorialPack == nil)
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
                DatasetExplorerPanel(
                    store: store,
                    datasetID: tab.datasetID,
                    initialMeasurementSetExplorerMode: initialMeasurementSetExplorerMode
                )
            case .tableBrowser:
                DatasetExplorerPanel(
                    store: store,
                    datasetID: tab.datasetID,
                    forceTableBrowser: true,
                    initialMeasurementSetExplorerMode: initialMeasurementSetExplorerMode
                )
            case .tutorial:
                TutorialPackPanel(store: store)
            case .task:
                TaskPanel(store: store, tab: tab)
                    .id(tab.id)
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
        case .tableBrowser: "tablecells"
        case .tutorial: "book"
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
                    if let url = TutorialPackOpenPanel.choosePack() {
                        store.openTutorialPack(path: url.path)
                    }
                } label: {
                    Label("Open Tutorial Pack", systemImage: "book")
                }
                .accessibilityIdentifier("empty.openTutorialPack")

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

struct TutorialPackPanel: View {
    @ObservedObject var store: WorkbenchStore

    var body: some View {
        ScrollView {
            if let context = store.state.tutorialPack {
                VStack(alignment: .leading, spacing: 18) {
                    PanelHeader(
                        title: context.title,
                        subtitle: "\(context.tutorialID) - CASA \(context.declaredCasaVersion)"
                    )

                    HStack(alignment: .top, spacing: 16) {
                        SummaryBox(
                            title: "Pack",
                            values: [
                                "pack_id=\(context.packID)",
                                "root=\(context.rootPath)",
                                "workspace=\(context.workspaceRoot)",
                                "review=\(context.reviewPath)"
                            ]
                        )
                        SummaryBox(
                            title: "Learner",
                            values: [
                                "docs=\(context.learnerDocsIndex)",
                                "sections=\(context.sections.count)"
                            ]
                        )
                    }

                    VStack(alignment: .leading, spacing: 10) {
                        Text("Inputs")
                            .workbenchFont(.headline)
                        ForEach(context.inputs) { input in
                            HStack(alignment: .firstTextBaseline, spacing: 8) {
                                Image(systemName: input.status == .staged ? "checkmark.circle.fill" : "exclamationmark.triangle.fill")
                                    .foregroundStyle(input.status == .staged ? .green : .orange)
                                VStack(alignment: .leading, spacing: 2) {
                                    Text(input.filename)
                                        .workbenchFont(.subheadline, weight: .semibold)
                                    Text(input.displayName)
                                        .workbenchFont(.caption)
                                        .foregroundStyle(.secondary)
                                    Text(input.packPath)
                                        .workbenchFont(.caption, design: .monospaced)
                                        .foregroundStyle(.secondary)
                                }
                                Spacer()
                                Text(input.status.rawValue)
                                    .workbenchFont(.caption, weight: .semibold)
                                    .foregroundStyle(input.status == .staged ? .green : .orange)
                            }
                            .accessibilityIdentifier("tutorial.input.\(input.id)")
                        }
                    }
                    .taskCard()

                    VStack(alignment: .leading, spacing: 10) {
                        Text("Sections")
                            .workbenchFont(.headline)
                        ForEach(context.sections) { section in
                            tutorialSectionRow(section, context: context)
                        }
                    }
                    .taskCard()
                }
                .padding(20)
            } else {
                VStack(alignment: .leading, spacing: 18) {
                    PanelHeader(title: "Tutorial Pack", subtitle: "Open a tutorial pack directory or pack.json")
                    Button {
                        if let url = TutorialPackOpenPanel.choosePack() {
                            store.openTutorialPack(path: url.path)
                        }
                    } label: {
                        Label("Open Tutorial Pack", systemImage: "book")
                    }
                    .accessibilityIdentifier("tutorial.openPack")
                }
                .padding(28)
                .frame(maxWidth: 560, alignment: .leading)
            }
        }
        .accessibilityIdentifier("panel.tutorialPack")
    }

    private func tutorialSectionRow(_ section: TutorialPackSection, context: TutorialPackContext) -> some View {
        let isSelected = context.selectedSection?.id == section.id
        return VStack(alignment: .leading, spacing: 8) {
            HStack(alignment: .firstTextBaseline) {
                VStack(alignment: .leading, spacing: 2) {
                    Text("\(section.sequence). \(section.title)")
                        .workbenchFont(.subheadline, weight: .semibold)
                    Text(section.observableResult)
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                Text(section.reviewCheckpoint.status)
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
            }
            HStack(spacing: 8) {
                ForEach(section.tasks, id: \.self) { task in
                    Text(task)
                        .workbenchFont(.caption, design: .monospaced)
                        .padding(.horizontal, 7)
                        .padding(.vertical, 3)
                        .background(Color.secondary.opacity(0.10))
                        .clipShape(RoundedRectangle(cornerRadius: 5))
                }
                Spacer()
                Button {
                    store.selectTutorialSection(section.id)
                } label: {
                    Label(isSelected ? "Selected" : "Select", systemImage: isSelected ? "checkmark.circle" : "circle")
                }
                .accessibilityIdentifier("tutorial.section.select.\(section.id)")

                Button {
                    store.openTutorialSectionTask(section.id)
                } label: {
                    Label("Open Task", systemImage: "slider.horizontal.3")
                }
                .accessibilityIdentifier("tutorial.section.openTask.\(section.id)")
            }
        }
        .padding(10)
        .background(isSelected ? Color.accentColor.opacity(0.12) : Color.clear)
        .clipShape(RoundedRectangle(cornerRadius: 8))
        .accessibilityIdentifier("tutorial.section.\(section.id)")
    }
}

struct DatasetExplorerPanel: View {
    @ObservedObject var store: WorkbenchStore
    let datasetID: String?
    var forceTableBrowser: Bool = false
    var initialMeasurementSetExplorerMode: MeasurementSetExplorerMode = .summary

    var body: some View {
        Group {
            if let dataset {
                if forceTableBrowser && !store.state.isDemoProject {
                    tableBrowserRoot(for: dataset)
                } else if dataset.kind == .measurementSet && !store.state.isDemoProject {
                    MeasurementSetPlotPanel(
                        store: store,
                        dataset: dataset,
                        initialExplorerMode: initialMeasurementSetExplorerMode
                    )
                } else if dataset.kind == .imageCube && !store.state.isDemoProject {
                    VStack(alignment: .leading, spacing: 10) {
                        PanelHeader(title: dataset.kind.explorerName, subtitle: explorerSubtitle(for: dataset))
                        realExplorerContent(for: dataset)
                            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
                        Text(dataset.path)
                            .workbenchFont(.caption, design: .monospaced)
                            .foregroundStyle(.secondary)
                            .lineLimit(1)
                    }
                    .padding(16)
                    .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
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

    private func tableBrowserRoot(for dataset: DatasetSummary) -> some View {
        VStack(alignment: .leading, spacing: 0) {
            tableBrowserToolbar(for: dataset)
                .padding(.horizontal, 12)
                .padding(.vertical, 8)
            Divider()
            tableExplorerContent(for: dataset)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .onAppear {
            let selectedView = store.state.tableBrowsers[dataset.id]?.selectedView
            if selectedView != nil && !Self.tableBrowserDisplayViews.contains(selectedView ?? "") {
                store.setTableBrowserView("cells", datasetID: dataset.id)
            }
        }
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
        case .region:
            regionExplorerContent(for: dataset)
        case .runProduct:
            productExplorerContent(for: dataset)
        }
    }

    @ViewBuilder
    private func regionExplorerContent(for dataset: DatasetSummary) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            SummaryBox(title: "Region File", values: [dataset.path])
            SummaryBox(title: "Use As Task Input", values: dataset.diagnostics)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }

    @ViewBuilder
    private func imageExplorerContent(for dataset: DatasetSummary) -> some View {
        let explorerState = store.state.imageExplorers[dataset.id]
        VStack(alignment: .leading, spacing: 8) {
            HStack(spacing: 8) {
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

            if let snapshot = explorerState?.snapshot {
                ImageExplorerSnapshotView(
                    store: store,
                    datasetID: dataset.id,
                    explorerState: explorerState,
                    snapshot: snapshot
                )
            } else {
                ImagePreviewPlaceholder(dataset: dataset)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .task(id: imageExplorerMovieTaskID(datasetID: dataset.id, state: explorerState)) {
            await runImageExplorerMovie(datasetID: dataset.id)
        }
    }

    private func imageExplorerMovieTaskID(datasetID: String, state: ImageExplorerSessionState?) -> String {
        guard let state else {
            return "\(datasetID)-movie-none"
        }
        return [
            datasetID,
            state.moviePlaying ? "playing" : "stopped",
            String(state.movieAxis ?? -1),
            String(format: "%.3f", state.movieFramesPerSecond),
            state.movieLoop ? "loop" : "once"
        ].joined(separator: ":")
    }

    @MainActor
    private func runImageExplorerMovie(datasetID: String) async {
        while !Task.isCancelled {
            guard let explorerState = store.state.imageExplorers[datasetID], explorerState.moviePlaying else {
                return
            }
            let framesPerSecond = min(max(explorerState.movieFramesPerSecond, 0.2), 60.0)
            let nanoseconds = UInt64((1.0 / framesPerSecond) * 1_000_000_000.0)
            do {
                try await Task.sleep(nanoseconds: nanoseconds)
            } catch {
                return
            }
            guard !Task.isCancelled else {
                return
            }
            store.advanceImageExplorerMovieFrame(datasetID: datasetID)
        }
    }

    @ViewBuilder
    private func tableExplorerContent(for dataset: DatasetSummary) -> some View {
        let browserState = store.state.tableBrowsers[dataset.id]
        let interfaceFontSize = store.state.interfaceFontSize
        Group {
            if let snapshot = browserState?.snapshot {
                TableBrowserSnapshotView(
                    snapshot: snapshot,
                    cellWindow: browserState?.cellWindow,
                    hiddenColumns: browserState?.hiddenCellColumns ?? [],
                    arrayInlineLimits: browserState?.cellColumnArrayInlineLimits ?? [:],
                    interfaceFontSize: interfaceFontSize,
                    selectedCellRow: browserState?.selectedCellRow,
                    selectedCellColumn: browserState?.selectedCellColumn,
                    selectMainItem: { index in store.selectTableBrowserMainItem(index: index, datasetID: dataset.id) },
                    selectCell: { rowIndex, selectedVisibleColumn, targetVisibleColumn in
                        store.selectTableBrowserVisibleCell(
                            rowIndex: rowIndex,
                            selectedVisibleColumn: selectedVisibleColumn,
                            targetVisibleColumn: targetVisibleColumn,
                            datasetID: dataset.id
                        )
                    },
                    requestCellWindow: { rowStart, rowLimit, columnStart, columnLimit in
                        store.requestTableBrowserCellWindow(
                            rowStart: rowStart,
                            rowLimit: rowLimit,
                            columnStart: columnStart,
                            columnLimit: columnLimit,
                            datasetID: dataset.id
                        )
                    },
                    setColumnHidden: { columnIndex, hidden in
                        store.setTableBrowserColumnHidden(columnIndex: columnIndex, hidden: hidden, datasetID: dataset.id)
                    },
                    setArrayInlineLimit: { columnIndex, limit in
                        store.setTableBrowserArrayInlineLimit(columnIndex: columnIndex, limit: limit, datasetID: dataset.id)
                    },
                    copyCellValue: { rowIndex, columnIndex in
                        store.loadTableBrowserCellValue(
                            rowIndex: rowIndex,
                            columnIndex: columnIndex,
                            datasetID: dataset.id
                        ) { result in
                            guard case let .success(value) = result else {
                                return
                            }
                            NSPasteboard.general.clearContents()
                            NSPasteboard.general.setString(value, forType: .string)
                        }
                    },
                    loadCellValue: { rowIndex, columnIndex, completion in
                        store.loadTableBrowserCellValue(
                            rowIndex: rowIndex,
                            columnIndex: columnIndex,
                            datasetID: dataset.id
                        ) { result in
                            completion(try? result.get())
                        }
                    },
                    openSelectedSubtable: {
                        store.openSelectedTableBrowserSubtable(datasetID: dataset.id)
                    }
                )
            } else {
                TablePreviewSummary(dataset: dataset)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
    }

    private func tableBrowserToolbar(for dataset: DatasetSummary) -> some View {
        let browserState = store.state.tableBrowsers[dataset.id]
        return HStack(spacing: 10) {
            Picker("View", selection: Binding(
                get: { Self.tableBrowserViewSelection(browserState?.selectedView) },
                set: { store.setTableBrowserView($0, datasetID: dataset.id) }
            )) {
                Text("Cells").tag("cells")
                Text("Keywords").tag("keywords")
                Text("Subtables").tag("subtables")
            }
            .pickerStyle(.segmented)
            .frame(width: 260)
            .labelsHidden()
            .accessibilityIdentifier("tableBrowser.view.\(dataset.id)")

            Button {
                store.refreshTableBrowser(datasetID: dataset.id)
            } label: {
                Image(systemName: "arrow.clockwise")
            }
            .buttonStyle(.borderless)
            .help("Refresh table")
            .accessibilityIdentifier("tableBrowser.refresh.\(dataset.id)")

            if let snapshot = browserState?.snapshot {
                Text(snapshot.breadcrumb.map(\.label).joined(separator: " / "))
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                if let address = tableBrowserAddressSummary(snapshot.selectedAddress) {
                    Text(address)
                        .workbenchFont(.caption, design: .monospaced)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                }
            } else if let error = browserState?.lastError {
                Text(error)
                    .workbenchFont(.caption)
                    .foregroundStyle(.red)
                    .lineLimit(1)
            } else {
                Text(dataset.path)
                    .workbenchFont(.caption, design: .monospaced)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }

            Spacer(minLength: 0)
        }
    }

    private static let tableBrowserDisplayViews = ["cells", "keywords", "subtables"]

    private static func tableBrowserViewSelection(_ view: String?) -> String {
        guard let view, tableBrowserDisplayViews.contains(view) else {
            return "cells"
        }
        return view
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
        case .region:
            "Region file"
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
        case .region:
            [dataset.path]
        case .calibrationTable, .table:
            [dataset.size, dataset.units]
        }
    }

    private func secondarySummaryTitle(for dataset: DatasetSummary) -> String {
        switch dataset.kind {
        case .measurementSet, .imageCube, .runProduct:
            "Spectral windows"
        case .region:
            "Task parameter"
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
        case .region:
            dataset.diagnostics
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
        case .region:
            [
                ExplorerPlot(title: "Region input", caption: "--region path"),
                ExplorerPlot(title: "Inline syntax", caption: "box[[x0pix,y0pix],[x1pix,y1pix]]")
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

private struct ImageExplorerControlsView: View {
    @ObservedObject var store: WorkbenchStore
    let datasetID: String
    let explorerState: ImageExplorerSessionState?
    let snapshot: ImageExplorerSnapshot?

    @State private var parameters: ImageExplorerParameters
    @State private var cursorXText: String
    @State private var cursorYText: String
    @State private var regionBoxText: String
    @State private var regionLoadText: String
    @State private var regionLoadStatus: String = ""
    @State private var coordinateEntryExpanded = false
    @State private var viewControlsExpanded = true
    @State private var regionControlsExpanded = true
    @State private var imageAttachedRegionsExpanded = false
    @State private var movieFPSText: String

    init(
        store: WorkbenchStore,
        datasetID: String,
        explorerState: ImageExplorerSessionState?,
        snapshot: ImageExplorerSnapshot?
    ) {
        self.store = store
        self.datasetID = datasetID
        self.explorerState = explorerState
        self.snapshot = snapshot
        let parameters = explorerState?.parameters ?? snapshot?.parameters ?? ImageExplorerParameters()
        _parameters = State(initialValue: parameters)
        _cursorXText = State(initialValue: String(explorerState?.cursorX ?? snapshot?.planeCursor?.pixelX ?? 0))
        _cursorYText = State(initialValue: String(explorerState?.cursorY ?? snapshot?.planeCursor?.pixelY ?? 0))
        _regionBoxText = State(initialValue: Self.defaultRegionBoxText(snapshot: snapshot))
        _regionLoadText = State(initialValue: Self.defaultRegionLoadText(store: store, imageDatasetID: datasetID))
        _movieFPSText = State(initialValue: Self.formatMovieFramesPerSecond(explorerState?.movieFramesPerSecond ?? 6.0))
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            DisclosureGroup("View", isExpanded: $viewControlsExpanded) {
                VStack(alignment: .leading, spacing: 8) {
                    controlsSection("Plane") {
                        modeAndCursorControls
                    }
                    controlsSection("Display") {
                        displayParameterControls
                    }
                    if snapshot?.nonDisplayAxes?.isEmpty == false {
                        controlsSection("Linked axes") {
                            movieControls
                            nonDisplayAxisControls
                        }
                    }
                }
            }
            DisclosureGroup("Regions and masks", isExpanded: $regionControlsExpanded) {
                regionMaskControls
            }
        }
        .workbenchFont(.caption)
        .onChange(of: snapshot?.planeCursor) { cursor in
            cursorXText = String(cursor?.pixelX ?? explorerState?.cursorX ?? 0)
            cursorYText = String(cursor?.pixelY ?? explorerState?.cursorY ?? 0)
        }
        .onChange(of: explorerState?.parameters) { nextParameters in
            if let nextParameters {
                parameters = nextParameters
            }
        }
        .onChange(of: explorerState?.movieFramesPerSecond) { framesPerSecond in
            if let framesPerSecond {
                movieFPSText = Self.formatMovieFramesPerSecond(framesPerSecond)
            }
        }
    }

    private func controlsSection<Content: View>(
        _ title: String,
        @ViewBuilder content: () -> Content
    ) -> some View {
        VStack(alignment: .leading, spacing: 5) {
            Text(title)
                .workbenchFont(.caption, weight: .semibold)
                .foregroundStyle(.secondary)
            content()
        }
    }

    private var modeAndCursorControls: some View {
        HStack(spacing: 8) {
            planeModePicker
            TextField("X", text: $cursorXText)
                .frame(width: 54)
                .textFieldStyle(.roundedBorder)
                .onSubmit(applyCursorText)
            TextField("Y", text: $cursorYText)
                .frame(width: 54)
                .textFieldStyle(.roundedBorder)
                .onSubmit(applyCursorText)
            Button {
                applyCursorText()
            } label: {
                Label("Set", systemImage: "scope")
            }
        }
    }

    private func applyCursorText() {
        store.setImageExplorerCursor(
            x: Int(cursorXText.trimmingCharacters(in: .whitespacesAndNewlines)),
            y: Int(cursorYText.trimmingCharacters(in: .whitespacesAndNewlines)),
            datasetID: datasetID
        )
    }

    private var planeModePicker: some View {
        Picker("Plane", selection: Binding(
            get: { explorerState?.planeContentMode ?? "raster" },
            set: { store.setImageExplorerPlaneContentMode($0, datasetID: datasetID) }
        )) {
            Text("Raster").tag("raster")
            Text("Spreadsheet").tag("spreadsheet")
        }
        .pickerStyle(.segmented)
        .frame(width: 190)
    }

    private var displayParameterControls: some View {
        HStack(spacing: 6) {
            TextField("BLC", text: $parameters.blc)
                .frame(width: 76)
            TextField("TRC", text: $parameters.trc)
                .frame(width: 76)
            TextField("INC", text: $parameters.inc)
                .frame(width: 66)
            colorMapPicker
            stretchPicker
            autoscalePicker
            TextField("Low", text: $parameters.clipLow)
                .frame(width: 72)
            TextField("High", text: $parameters.clipHigh)
                .frame(width: 72)
            Button {
                store.setImageExplorerParameters(parameters, datasetID: datasetID)
            } label: {
                Label("Apply", systemImage: "slider.horizontal.3")
            }
        }
        .textFieldStyle(.roundedBorder)
    }

    private var colorMapPicker: some View {
        Picker("Colormap", selection: Binding(
            get: { explorerState?.planeColorMap ?? .grayscale },
            set: { store.setImageExplorerColorMap($0, datasetID: datasetID) }
        )) {
            ForEach(ImageExplorerColorMap.allCases) { colorMap in
                Text(colorMap.label).tag(colorMap)
            }
        }
        .pickerStyle(.menu)
        .frame(width: 120)
    }

    private var stretchPicker: some View {
        Picker("Stretch", selection: $parameters.stretch) {
            Text("Percentile 99").tag("percentile99")
            Text("Percentile 95").tag("percentile95")
            Text("Min/Max").tag("minmax")
            Text("ZScale").tag("zscale")
            Text("Manual").tag("manual")
        }
        .pickerStyle(.menu)
        .frame(width: 130)
    }

    private var autoscalePicker: some View {
        Picker("Autoscale", selection: $parameters.autoscale) {
            Text("Per plane").tag("per_plane")
            Text("Frozen").tag("frozen")
        }
        .pickerStyle(.menu)
        .frame(width: 105)
    }

    @ViewBuilder
    private var movieControls: some View {
        if let axes = snapshot?.nonDisplayAxes, !axes.isEmpty {
            VStack(alignment: .leading, spacing: 6) {
                HStack(spacing: 6) {
                    Label("Movie", systemImage: "film")
                    TextField("FPS", text: $movieFPSText)
                        .frame(width: 64)
                        .textFieldStyle(.roundedBorder)
                    Button {
                        if let framesPerSecond = Double(movieFPSText.trimmingCharacters(in: .whitespacesAndNewlines)) {
                            store.setImageExplorerMovieFramesPerSecond(framesPerSecond, datasetID: datasetID)
                            movieFPSText = Self.formatMovieFramesPerSecond(framesPerSecond)
                        }
                    } label: {
                        Label("Set", systemImage: "speedometer")
                    }
                    Toggle("Loop", isOn: Binding(
                        get: { explorerState?.movieLoop ?? true },
                        set: { store.setImageExplorerMovieLoop($0, datasetID: datasetID) }
                    ))
                    .toggleStyle(.checkbox)
                }
                HStack(spacing: 6) {
                    if explorerState?.moviePlaying == true {
                        Button {
                            store.stopImageExplorerMovie(datasetID: datasetID)
                        } label: {
                            Label("Stop", systemImage: "stop.fill")
                        }
                    }
                    Text(movieStatusText(axes: axes))
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                }
            }
            .controlSize(.small)
        }
    }

    @ViewBuilder
    private var nonDisplayAxisControls: some View {
        if let axes = snapshot?.nonDisplayAxes, !axes.isEmpty {
            VStack(alignment: .leading, spacing: 6) {
                ForEach(axes) { axis in
                    nonDisplayAxisControl(axis)
                }
            }
        }
    }

    private func nonDisplayAxisControl(_ axis: ImageExplorerSnapshot.NonDisplayAxis) -> some View {
        HStack(spacing: 4) {
            Button {
                toggleMovie(axis: axis.axis)
            } label: {
                Image(systemName: isMoviePlaying(axis: axis.axis) ? "pause.fill" : "play.fill")
            }
            .help(isMoviePlaying(axis: axis.axis) ? "Pause movie playback" : "Play this axis as a movie")
            Button {
                store.stepImageExplorerNonDisplayAxis(axis: axis.axis, delta: -1, datasetID: datasetID)
            } label: {
                Image(systemName: "chevron.left")
            }
            Text("\(axis.label): \(axis.index + 1)/\(axis.length)")
                .workbenchFont(.caption)
                .frame(maxWidth: .infinity, alignment: .leading)
            Button {
                store.stepImageExplorerNonDisplayAxis(axis: axis.axis, delta: 1, datasetID: datasetID)
            } label: {
                Image(systemName: "chevron.right")
            }
            Button {
                store.setImageExplorerSelectedProfileAxis(axis.axis, datasetID: datasetID)
            } label: {
                Image(systemName: "waveform.path.ecg")
            }
            .help("Use this axis for the linked profile plot")
        }
        .controlSize(.small)
    }

    private func isMoviePlaying(axis: Int) -> Bool {
        explorerState?.moviePlaying == true && explorerState?.movieAxis == axis
    }

    private func toggleMovie(axis: Int) {
        if isMoviePlaying(axis: axis) {
            store.stopImageExplorerMovie(datasetID: datasetID)
            return
        }
        let framesPerSecond = Double(movieFPSText.trimmingCharacters(in: .whitespacesAndNewlines))
            ?? explorerState?.movieFramesPerSecond
            ?? 6.0
        store.startImageExplorerMovie(
            axis: axis,
            framesPerSecond: framesPerSecond,
            loop: explorerState?.movieLoop ?? true,
            datasetID: datasetID
        )
        movieFPSText = Self.formatMovieFramesPerSecond(framesPerSecond)
    }

    private func movieStatusText(axes: [ImageExplorerSnapshot.NonDisplayAxis]) -> String {
        guard explorerState?.moviePlaying == true else {
            return "Stopped"
        }
        let axisID = explorerState?.movieAxis ?? axes.first?.axis
        let axis = axes.first { $0.axis == axisID }
        let label = axis?.label ?? "Axis \(axisID ?? 0)"
        let index = (axis?.index ?? 0) + 1
        let length = axis?.length ?? 1
        return "\(label) \(index)/\(length) at \(Self.formatMovieFramesPerSecond(explorerState?.movieFramesPerSecond ?? 6.0)) fps"
    }

    private static func formatMovieFramesPerSecond(_ framesPerSecond: Double) -> String {
        let clamped = min(max(framesPerSecond.isFinite ? framesPerSecond : 6.0, 0.2), 60.0)
        if clamped.rounded() == clamped {
            return String(Int(clamped))
        }
        return String(format: "%.1f", clamped)
    }

    private static func defaultRegionBoxText(snapshot: ImageExplorerSnapshot?) -> String {
        guard let width = snapshot?.shape.first, width > 4,
              snapshot?.shape.count ?? 0 > 1,
              let height = snapshot?.shape[1], height > 4
        else {
            return ""
        }
        let x0 = Int(width) * 2 / 5
        let x1 = Int(width) * 3 / 5
        let y0 = Int(height) * 2 / 5
        let y1 = Int(height) * 3 / 5
        return "\(x0),\(y0),\(x1),\(y1)"
    }

    private static func defaultRegionLoadText(store: WorkbenchStore, imageDatasetID: String) -> String {
        let selected = store.state.selectedDataset
        if selected?.kind == .region {
            return selected?.path ?? ""
        }
        let imageDataset = store.state.project.datasets.first { $0.id == imageDatasetID }
        if let imageDataset {
            let sourceNeedle = "Region source image:"
            let imagePaths = [
                imageDataset.path,
                Self.projectRelativePath(imageDataset.path, rootPath: store.state.project.rootPath)
            ]
            if let region = store.state.project.datasets.first(where: { dataset in
                guard dataset.kind == .region else {
                    return false
                }
                return dataset.diagnostics.contains { diagnostic in
                    guard diagnostic.hasPrefix(sourceNeedle) else {
                        return false
                    }
                    let value = diagnostic.dropFirst(sourceNeedle.count).trimmingCharacters(in: .whitespacesAndNewlines)
                    return imagePaths.contains(String(value))
                }
            }) {
                return region.path
            }
        }
        return store.state.project.datasets.first { $0.kind == .region }?.path ?? ""
    }

    private static func projectRelativePath(_ path: String, rootPath: String) -> String {
        let trimmed = path.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty, !rootPath.isEmpty else {
            return path
        }
        let rootURL = URL(fileURLWithPath: rootPath, isDirectory: true).standardizedFileURL
        let pathURL = URL(fileURLWithPath: (trimmed as NSString).expandingTildeInPath).standardizedFileURL
        let root = rootURL.path
        let absolutePath = pathURL.path
        let prefix = root.hasSuffix("/") ? root : root + "/"
        if absolutePath.hasPrefix(prefix) {
            return String(absolutePath.dropFirst(prefix.count))
        }
        return path
    }

    private var regionMaskControls: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(spacing: 8) {
                Menu {
                    Button {
                        store.setImageExplorerRegionTool("box", datasetID: datasetID)
                    } label: {
                        Label("Box", systemImage: "rectangle")
                    }
                    Button {
                        store.setImageExplorerRegionTool("polygon", datasetID: datasetID)
                    } label: {
                        Label("Polygon", systemImage: "pentagon")
                    }
                } label: {
                    Label("New...", systemImage: "plus")
                }
                .help("Start drawing a new region.")

                DatasetPathInputControl(
                    store: store,
                    label: "Region",
                    value: $regionLoadText,
                    help: "CASA CRTF region file to load into this Image Explorer pane.",
                    choices: regionPathChoices(),
                    canBrowse: true,
                    actionTitle: nil,
                    actionSystemImage: "square.and.arrow.up",
                    allowInlineSyntax: false,
                    syncSelectedRegion: true,
                    onAction: nil
                )
                .frame(minWidth: 240)

                Button {
                    loadRegionFile(resolvedRegionLoadPath(), replacing: false)
                } label: {
                    Label("Load", systemImage: "square.and.arrow.up")
                }
                .disabled(resolvedRegionLoadPath().trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                .help("Add the selected region file to this Image Explorer pane.")

                Button {
                    loadRegionFile(resolvedRegionLoadPath(), replacing: true)
                } label: {
                    Label("Replace", systemImage: "arrow.triangle.2.circlepath")
                }
                .disabled(resolvedRegionLoadPath().trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                .help("Replace the active region with the selected region file.")

                Button {
                    store.setImageExplorerRegionTool("select", datasetID: datasetID)
                } label: {
                    Label("Select", systemImage: "cursorarrow")
                }
                .disabled((explorerState?.regionTool ?? "select") == "select")

                Button {
                    store.exportImageExplorerRegionFile(datasetID: datasetID)
                } label: {
                    Label("Save", systemImage: "square.and.arrow.down")
                }
                .help("Write the active region to the tutorial workspace and add it to the dataset list.")

                regionDeleteMenu

                Menu("Mask") {
                    Button("Create mask from region") {
                        store.runImageExplorerCommandOnce(.writeRegionMask(name: nil, setDefault: true), datasetID: datasetID)
                    }
                    Button("Unset default mask") {
                        store.runImageExplorerCommandOnce(.unsetDefaultMask, datasetID: datasetID)
                    }
                    ForEach(snapshot?.maskNames ?? [], id: \.self) { name in
                        Button("Set \(name) default") {
                            store.runImageExplorerCommandOnce(.setDefaultMask(name: name), datasetID: datasetID)
                        }
                        Button("Delete \(name)") {
                            store.runImageExplorerCommandOnce(.deleteMask(name: name), datasetID: datasetID)
                        }
                    }
                }
            }
            Text(activeRegionSummary)
                .foregroundStyle(.secondary)
                .lineLimit(1)
            if !regionLoadStatus.isEmpty {
                Text(regionLoadStatus)
                    .foregroundStyle(regionLoadStatus.hasPrefix("Region load failed") ? .red : .secondary)
                    .lineLimit(2)
            }
            HStack(spacing: 6) {
                Button {
                    store.appendImageExplorerRegionCommand(.closeRegionShape, datasetID: datasetID)
                    store.setImageExplorerRegionTool("select", datasetID: datasetID)
                } label: {
                    Label("Finish", systemImage: "checkmark")
                }
                .help("Close the active polygon. You can also click near the first vertex.")
                Button {
                    store.appendImageExplorerRegionCommand(.undoRegionVertex, datasetID: datasetID)
                } label: {
                    Label("Undo Point", systemImage: "arrow.uturn.backward")
                }
                Button {
                    store.appendImageExplorerRegionCommand(.cancelRegionShape, datasetID: datasetID)
                } label: {
                    Label("Cancel Drawing", systemImage: "xmark")
                }
            }
            DisclosureGroup("Coordinate entry", isExpanded: $coordinateEntryExpanded) {
                HStack(spacing: 6) {
                    TextField("x0,y0,x1,y1", text: $regionBoxText)
                        .frame(width: 140)
                        .textFieldStyle(.roundedBorder)
                        .onSubmit(createBoxRegion)
                    Button {
                        createBoxRegion()
                    } label: {
                        Label("Create Box Region", systemImage: "rectangle")
                    }
                    Text("Pixel coordinates")
                        .foregroundStyle(.secondary)
                }
            }
            DisclosureGroup("Image-attached regions", isExpanded: $imageAttachedRegionsExpanded) {
                imageAttachedRegionControls
            }
            .help("Native CASA/casacore regions stored inside the image table, separate from CRTF region files.")
            Text(regionToolHint)
                .foregroundStyle(.secondary)
                .lineLimit(2)
        }
        .controlSize(.small)
    }

    private var regionDeleteMenu: some View {
        Menu {
            Button("Delete Last Shape") {
                store.deleteLastImageExplorerRegionShape(datasetID: datasetID)
                regionLoadStatus = "Deleted the last region shape."
            }
            .disabled(!hasActiveRegionShapes)
            ForEach(Array((snapshot?.region?.overlayShapes ?? []).indices), id: \.self) { index in
                Button("Delete Shape \(index + 1)") {
                    store.deleteImageExplorerRegionShape(index: index, datasetID: datasetID)
                    regionLoadStatus = "Deleted region shape \(index + 1)."
                }
            }
            Divider()
            Button("Delete All Shapes") {
                store.clearImageExplorerRegionCommands(datasetID: datasetID)
                regionLoadStatus = "Deleted all region shapes."
            }
            .disabled(!hasActiveRegionShapes)
        } label: {
            Label("Delete", systemImage: "trash")
        }
        .disabled(!hasActiveRegionShapes)
    }

    private var hasActiveRegionShapes: Bool {
        (snapshot?.region?.overlayShapes?.isEmpty == false)
    }

    private var imageAttachedRegionControls: some View {
        HStack(spacing: 6) {
            Button("Save to Image") {
                store.runImageExplorerCommandOnce(.saveRegionDefinition, datasetID: datasetID)
            }
            Button("Load Next") {
                store.runImageExplorerCommandOnce(.loadNextRegionDefinition, datasetID: datasetID)
            }
            .disabled(snapshot?.savedRegionNames.isEmpty ?? true)
            Menu("Saved") {
                ForEach(snapshot?.savedRegionNames ?? [], id: \.self) { name in
                    Button("Load \(name)") {
                        store.runImageExplorerCommandOnce(.loadRegionDefinition(name: name), datasetID: datasetID)
                    }
                    Button("Delete \(name)") {
                        store.runImageExplorerCommandOnce(.deleteRegionDefinition(name: name), datasetID: datasetID)
                    }
                }
            }
            .disabled(snapshot?.savedRegionNames.isEmpty ?? true)
        }
    }

    private var regionToolHint: String {
        switch explorerState?.regionTool ?? "select" {
        case "box":
            return "New box: click one corner, move the pointer to preview the outline, then click the opposite corner. Esc cancels."
        case "polygon":
            return "New polygon: click to place vertices, move the pointer to preview the next edge, and click near the first vertex to close. Esc cancels."
        default:
            return "Select: drag a vertex to resize, or drag inside the shape to move the region."
        }
    }

    private var activeRegionSummary: String {
        guard let region = snapshot?.region else {
            return "No active region loaded."
        }
        let vertices = region.overlayShapes?.reduce(0) { $0 + $1.vertices.count } ?? 0
        let persistence = activeRegionIsSaved ? "Saved region" : "Unsaved region"
        if let stats = region.stats {
            return "\(persistence): \(region.label), \(region.closedShapeCount) shape(s), \(vertices) vertices, \(stats.pixelCount) pixels."
        }
        return "\(persistence): \(region.label), \(region.closedShapeCount) shape(s), \(vertices) vertices."
    }

    private var activeRegionIsSaved: Bool {
        guard snapshot?.region != nil else {
            return false
        }
        guard let commands = explorerState?.regionCommands, !commands.isEmpty else {
            return snapshot?.activeRegionDefinitionName != nil
        }
        guard commands.count == 1, let command = commands.first?.command else {
            return false
        }
        return command == "load_region_file" || command == "append_region_file"
    }

    private func currentCursor() -> (x: Int, y: Int) {
        (
            Int(cursorXText.trimmingCharacters(in: .whitespacesAndNewlines)) ?? snapshot?.planeCursor?.pixelX ?? 0,
            Int(cursorYText.trimmingCharacters(in: .whitespacesAndNewlines)) ?? snapshot?.planeCursor?.pixelY ?? 0
        )
    }

    private func createBoxRegion() {
        store.appendImageExplorerBoxRegion(regionBoxText, datasetID: datasetID)
    }

    private func loadRegionFile(_ path: String, replacing: Bool) {
        let trimmed = path.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            regionLoadStatus = "Region load failed: no region file path was selected."
            return
        }
        guard FileManager.default.fileExists(atPath: trimmed) else {
            regionLoadStatus = "Region load failed: \(displayRegionPath(trimmed)) does not exist."
            return
        }
        let previousShapeCount = snapshot?.region?.closedShapeCount ?? 0
        if replacing {
            store.loadImageExplorerRegionFile(path: path, datasetID: datasetID)
        } else {
            store.appendImageExplorerRegionFile(path: path, datasetID: datasetID)
        }
        store.setImageExplorerRegionTool("select", datasetID: datasetID)
        if let region = store.state.imageExplorers[datasetID]?.snapshot?.region {
            let commands = store.state.imageExplorers[datasetID]?.regionCommands ?? []
            let reloadedFile = commands.count == 1 && commands.first?.command == "load_region_file"
            let verb = replacing ? "Replaced with" : (reloadedFile && previousShapeCount > 0 ? "Reloaded" : (previousShapeCount > 0 ? "Added" : "Loaded"))
            regionLoadStatus = "\(verb) \(displayRegionPath(trimmed)): \(region.closedShapeCount) shape(s)."
        } else if let error = store.state.imageExplorers[datasetID]?.lastError {
            regionLoadStatus = "Region load failed: \(error)"
        } else if let error = store.state.lastErrors.last {
            regionLoadStatus = "Region load failed: \(error)"
        } else {
            regionLoadStatus = "Region load did not produce an active region."
        }
    }

    private func resolvedRegionLoadPath() -> String {
        let trimmed = regionLoadText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return regionLoadText
        }
        let expanded = (trimmed as NSString).expandingTildeInPath
        if expanded.hasPrefix("/") {
            return expanded
        }
        let root = store.state.project.rootPath.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !root.isEmpty else {
            return regionLoadText
        }
        return URL(fileURLWithPath: root, isDirectory: true)
            .appendingPathComponent(trimmed)
            .standardizedFileURL
            .path
    }

    private func regionPathChoices() -> [DatasetPathChoice] {
        store.state.project.datasets
            .filter { $0.kind == .region }
            .sorted { $0.name.localizedStandardCompare($1.name) == .orderedAscending }
            .map { DatasetPathChoice(name: $0.name, path: $0.path) }
    }

    private func displayRegionPath(_ path: String) -> String {
        Self.projectRelativePath(path, rootPath: store.state.project.rootPath)
    }
}

private struct ImageExplorerSnapshotView: View {
    @ObservedObject var store: WorkbenchStore
    let datasetID: String
    let explorerState: ImageExplorerSessionState?
    let snapshot: ImageExplorerSnapshot
    @State private var controlsExpanded = false

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            VStack(alignment: .leading, spacing: 0) {
                HStack(spacing: 8) {
                    Button {
                        controlsExpanded.toggle()
                    } label: {
                        HStack(spacing: 5) {
                            Image(systemName: controlsExpanded ? "chevron.down" : "chevron.right")
                            Image(systemName: "slider.horizontal.3")
                            Text("Controls")
                        }
                    }
                    .buttonStyle(.plain)
                    .help(controlsExpanded ? "Hide display controls" : "Show display controls")

                    quickMovieControls
                    quickColorMapControl

                    Spacer()

                    Text(controlSummary)
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                }
                .padding(.horizontal, 10)
                .padding(.vertical, 7)

                if controlsExpanded {
                    Divider()
                    ImageExplorerControlsView(
                        store: store,
                        datasetID: datasetID,
                        explorerState: explorerState,
                        snapshot: snapshot
                    )
                    .padding(10)
                }
            }
            .workbenchFont(.caption)
            .controlSize(.small)
            .background(.regularMaterial)
            .clipShape(RoundedRectangle(cornerRadius: 6))

            ImageExplorerImageWorkspaceView(
                store: store,
                datasetID: datasetID,
                snapshot: snapshot,
                regionTool: explorerState?.regionTool ?? "select",
                regionIsSaved: Self.regionIsSaved(explorerState: explorerState, snapshot: snapshot),
                colorMap: explorerState?.planeColorMap ?? .grayscale
            )
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .top)
        }
        .accessibilityIdentifier("imageExplorer.snapshot")
    }

    private static func regionIsSaved(
        explorerState: ImageExplorerSessionState?,
        snapshot: ImageExplorerSnapshot
    ) -> Bool {
        guard snapshot.region != nil else {
            return false
        }
        guard let commands = explorerState?.regionCommands, !commands.isEmpty else {
            return snapshot.activeRegionDefinitionName != nil
        }
        guard commands.count == 1, let command = commands.first?.command else {
            return false
        }
        return command == "load_region_file" || command == "append_region_file"
    }

    private var quickMovieControls: some View {
        HStack(spacing: 2) {
            Button {
                startQuickMovie()
            } label: {
                Image(systemName: "play.fill")
            }
            .disabled(!canStartQuickMovie)
            .accessibilityLabel("Start movie")
            .help(canStartQuickMovie ? "Start movie playback" : "No movie axis available")

            Button {
                store.stopImageExplorerMovie(datasetID: datasetID)
            } label: {
                Image(systemName: "stop.fill")
            }
            .disabled(explorerState?.moviePlaying != true)
            .accessibilityLabel("Stop movie")
            .help("Stop movie playback")
        }
        .buttonStyle(.borderless)
    }

    private var quickColorMapControl: some View {
        Button {
            store.cycleImageExplorerColorMap(datasetID: datasetID)
        } label: {
            Image(systemName: "paintpalette")
        }
        .buttonStyle(.borderless)
        .keyboardShortcut("c", modifiers: [])
        .accessibilityLabel("Cycle colormap")
        .help("Cycle colormap (C)")
    }

    private var canStartQuickMovie: Bool {
        quickMovieAxis != nil && explorerState?.moviePlaying != true
    }

    private var quickMovieAxis: Int? {
        let axes = snapshot.nonDisplayAxes ?? []
        if let movieAxis = explorerState?.movieAxis, axes.contains(where: { $0.axis == movieAxis }) {
            return movieAxis
        }
        if let profileAxis = explorerState?.selectedProfileAxis, axes.contains(where: { $0.axis == profileAxis }) {
            return profileAxis
        }
        return axes.first?.axis
    }

    private func startQuickMovie() {
        guard let axis = quickMovieAxis else {
            return
        }
        store.startImageExplorerMovie(
            axis: axis,
            framesPerSecond: explorerState?.movieFramesPerSecond ?? 6.0,
            loop: explorerState?.movieLoop ?? true,
            datasetID: datasetID
        )
    }

    private var controlSummary: String {
        let mode = explorerState?.planeContentMode ?? "raster"
        let axes = snapshot.nonDisplayAxes ?? []
        if explorerState?.moviePlaying == true {
            let axisID = explorerState?.movieAxis ?? axes.first?.axis
            let axis = axes.first { $0.axis == axisID }
            let label = axis?.label ?? "axis \(axisID ?? 0)"
            return "\(mode), movie \(label)"
        }
        if let cursor = snapshot.planeCursor {
            return "\(mode), cursor \(cursor.pixelX),\(cursor.pixelY)"
        }
        return mode
    }
}

private struct ImageExplorerImageWorkspaceView: View {
    @ObservedObject var store: WorkbenchStore
    let datasetID: String
    let snapshot: ImageExplorerSnapshot
    let regionTool: String
    let regionIsSaved: Bool
    let colorMap: ImageExplorerColorMap

    var body: some View {
        GeometryReader { geometry in
            let profileHeight = profileHeight(for: geometry.size)
            VStack(alignment: .leading, spacing: 0) {
                if let plane = snapshot.plane {
                    ImagePlaneRasterView(
                        plane: plane,
                        cursor: snapshot.planeCursor,
                        region: snapshot.region,
                        displayAxes: snapshot.displayAxes ?? [],
                        probe: snapshot.probe,
                        nonDisplayAxes: snapshot.nonDisplayAxes ?? [],
                        regionTool: regionTool,
                        regionIsSaved: regionIsSaved,
                        colorMap: colorMap
                    ) { interaction in
                        handleImagePlaneInteraction(interaction)
                    } onClipRangeChange: { low, high in
                        store.setImageExplorerManualClip(low: low, high: high, datasetID: datasetID)
                    }
                    .id(rasterRenderIdentity)
                    .frame(height: max(1, geometry.size.height - profileHeight))
                } else {
                    Text("No renderable plane in current image browser snapshot.")
                        .foregroundStyle(.secondary)
                        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .center)
                }

                if let profile = snapshot.profile {
                    ImageProfilePanelView(profile: profile) { axis, sampleIndex in
                        store.setImageExplorerNonDisplayAxisIndex(
                            axis: axis,
                            index: sampleIndex,
                            datasetID: datasetID
                        )
                    }
                    .frame(height: profileHeight)
                    .border(Color.secondary.opacity(0.18), width: 1)
                }
            }
            .frame(width: geometry.size.width, height: geometry.size.height, alignment: .top)
        }
        .frame(minHeight: 520)
    }

    private func profileHeight(for size: CGSize) -> CGFloat {
        guard snapshot.profile != nil else {
            return 0
        }
        let preferred = size.height * 0.26
        let maximum = min(210, size.height * 0.42)
        return min(max(150, preferred), maximum)
    }

    private var rasterRenderIdentity: String {
        let regionIdentity: String
        if let region = snapshot.region {
            let vertices = region.overlayShapes?.reduce(0) { $0 + $1.vertices.count } ?? 0
            regionIdentity = "\(region.label)-\(region.shapeCount)-\(region.closedShapeCount)-\(vertices)"
        } else {
            regionIdentity = "no-region"
        }
        return "\(colorMap.rawValue)-\(regionIdentity)"
    }

    private func handleImagePlaneInteraction(_ interaction: ImagePlaneInteraction) {
        switch interaction {
        case .selectPixel(let x, let y):
            store.setImageExplorerCursor(x: x, y: y, datasetID: datasetID)
        case .appendPolygonVertex(let x, let y):
            if snapshot.region?.editing != true {
                store.appendImageExplorerRegionCommand(.startRegionShape, datasetID: datasetID)
            }
            store.appendImageExplorerRegionCommand(.appendRegionVertex(x: x, y: y), datasetID: datasetID)
        case .closePolygon:
            store.appendImageExplorerRegionCommand(.closeRegionShape, datasetID: datasetID)
            store.setImageExplorerRegionTool("select", datasetID: datasetID)
        case .createRectangle(let x0, let y0, let x1, let y1):
            store.appendImageExplorerBoxRegion("\(x0),\(y0),\(x1),\(y1)", datasetID: datasetID)
            store.setImageExplorerRegionTool("select", datasetID: datasetID)
        case .replaceRegionShapes(let shapes):
            store.setImageExplorerRegionShapes(shapes, datasetID: datasetID)
        case .deleteRegionShape(let index):
            store.deleteImageExplorerRegionShape(index: index, datasetID: datasetID)
        case .rejectRegionEdit(let message):
            store.reportImageExplorerRegionError(message)
        }
    }
}

private struct ImagePlaneKeyCaptureView: NSViewRepresentable {
    let focusNonce: Int
    let onDelete: () -> Bool

    func makeNSView(context: Context) -> KeyCaptureNSView {
        let view = KeyCaptureNSView()
        view.onDelete = onDelete
        return view
    }

    func updateNSView(_ nsView: KeyCaptureNSView, context: Context) {
        nsView.onDelete = onDelete
        guard focusNonce != context.coordinator.lastFocusNonce else {
            return
        }
        context.coordinator.lastFocusNonce = focusNonce
        DispatchQueue.main.async {
            nsView.window?.makeFirstResponder(nsView)
        }
    }

    func makeCoordinator() -> Coordinator {
        Coordinator()
    }

    final class Coordinator {
        var lastFocusNonce = 0
    }
}

private final class KeyCaptureNSView: NSView {
    var onDelete: (() -> Bool)?

    override var acceptsFirstResponder: Bool {
        true
    }

    override func keyDown(with event: NSEvent) {
        if event.keyCode == 51 || event.keyCode == 117 {
            if onDelete?() == true {
                return
            }
        }
        super.keyDown(with: event)
    }
}

private struct ImageProfilePanelView: View {
    let profile: ImageExplorerSnapshot.Profile
    let onSampleSelect: (Int, Int) -> Void
    @Environment(\.workbenchFontSize) private var workbenchFontSize

    var body: some View {
        GeometryReader { geometry in
            let reservedRightGutter = ImagePlaneViewportGeometry.profileReservedRightGutter(
                for: geometry.size,
                characterSize: workbenchFontSize
            )
            ZStack(alignment: .topLeading) {
                WorkbenchPlotView(
                    plot: profilePlotDocument(profile),
                    reservedRightGutter: reservedRightGutter
                )
                Color.clear
                    .contentShape(Rectangle())
                    .gesture(
                        DragGesture(minimumDistance: 0)
                            .onEnded { value in
                                if let sampleIndex = sampleIndex(at: value.location, size: geometry.size, reservedRightGutter: reservedRightGutter) {
                                    onSampleSelect(profile.axis, sampleIndex)
                                }
                            }
                    )
            }
        }
    }

    private func sampleIndex(at location: CGPoint, size: CGSize, reservedRightGutter: CGFloat) -> Int? {
        let plotRect = WorkbenchPlotLayout.plotRect(
            for: size,
            characterSize: workbenchFontSize,
            reservedRightGutter: reservedRightGutter
        )
        guard plotRect.contains(location), !profile.samples.isEmpty else {
            return nil
        }
        let slot = imageClickSampleIndex(
            relative: location.x - plotRect.minX,
            drawLength: plotRect.width,
            sampledLength: profile.samples.count
        )
        return profile.samples[slot].sampleIndex
    }

    private func profilePlotDocument(_ profile: ImageExplorerSnapshot.Profile) -> WorkbenchPlotDocument {
        let xAxisPresentation = profileXAxisPresentation(profile)
        let points = profile.samples
            .filter { $0.finite && $0.masked != true }
            .map { sample in
                WorkbenchPlotPoint(x: xAxisPresentation.value(for: sample), y: sample.value)
            }
        let selectedSample = profile.samples.first { $0.sampleIndex == profile.selectedSampleIndex }
        let selectedX = selectedSample.map { xAxisPresentation.value(for: $0) }
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
        let selectedLayer = selectedX.map { xValue in
            WorkbenchPlotLayer(
                id: "profile-\(profile.axis)-selected",
                title: "selected",
                kind: .scatter,
                xAxisID: "sample",
                yAxisID: "value",
                points: [WorkbenchPlotPoint(x: xValue, y: selectedSample?.value ?? 0)],
                style: WorkbenchPlotLayerStyle(colorHex: "#f59e0b", symbolSize: 6.0, opacity: 0.95),
                provenanceSummary: "Current image plane frame."
            )
        }
        let xRange = WorkbenchPlotRange(
            lower: points.map(\.x).min() ?? 0,
            upper: points.map(\.x).max() ?? 1
        )
        let yRange = WorkbenchPlotRange(
            lower: points.map(\.y).min() ?? 0,
            upper: points.map(\.y).max() ?? 1
        )
        var layers = [layer]
        if let selectedLayer {
            layers.append(selectedLayer)
        }
        return WorkbenchPlotDocument(
            id: "image-profile-\(profile.axis)",
            title: "\(profile.axisName) Profile",
            subtitle: "\(profile.valueUnit) vs \(xAxisPresentation.unit)",
            axes: [
                WorkbenchPlotAxis(id: "sample", label: profile.axisName, unit: xAxisPresentation.unit, range: expandedRange(xRange)),
                WorkbenchPlotAxis(id: "value", label: "Value", unit: profile.valueUnit, range: expandedRange(yRange))
            ],
            layers: layers,
            showLegend: false
        )
    }

    private func profileXAxisPresentation(_ profile: ImageExplorerSnapshot.Profile) -> ProfileXAxisPresentation {
        let worldValues = profile.samples.compactMap { $0.worldAxis?.value }.filter { $0.isFinite }
        let worldUnit = profile.samples.compactMap { $0.worldAxis?.unit }.first
        let unit = worldUnit ?? profile.axisUnit
        guard !worldValues.isEmpty, unit.compare("Hz", options: .caseInsensitive) == .orderedSame else {
            return ProfileXAxisPresentation(scale: 1, unit: unit)
        }
        let maxAbs = worldValues.map(abs).max() ?? 0
        if maxAbs >= 1e9 {
            return ProfileXAxisPresentation(scale: 1e9, unit: "GHz")
        }
        if maxAbs >= 1e6 {
            return ProfileXAxisPresentation(scale: 1e6, unit: "MHz")
        }
        if maxAbs >= 1e3 {
            return ProfileXAxisPresentation(scale: 1e3, unit: "kHz")
        }
        return ProfileXAxisPresentation(scale: 1, unit: "Hz")
    }

    private func expandedRange(_ range: WorkbenchPlotRange) -> WorkbenchPlotRange {
        guard range.lower == range.upper else {
            return range
        }
        return WorkbenchPlotRange(lower: range.lower - 0.5, upper: range.upper + 0.5)
    }
}

private struct ProfileXAxisPresentation {
    let scale: Double
    let unit: String

    func value(for sample: ImageExplorerSnapshot.Profile.Sample) -> Double {
        guard let worldValue = sample.worldAxis?.value else {
            return Double(sample.sampleIndex)
        }
        return worldValue / scale
    }
}

private enum ImagePlaneInteraction {
    case selectPixel(Int, Int)
    case appendPolygonVertex(Int, Int)
    case closePolygon
    case createRectangle(Int, Int, Int, Int)
    case replaceRegionShapes([[(x: Int, y: Int)]])
    case deleteRegionShape(Int)
    case rejectRegionEdit(String)
}

private struct ImagePlaneVertexSelection {
    let shapeIndex: Int
    let vertexIndex: Int
}

private struct ImagePlaneRegionDrag {
    enum Kind {
        case vertex(ImagePlaneVertexSelection)
        case shape(Int)
    }

    let kind: Kind
    let startPixel: (x: Int, y: Int)
    let baseVertices: [(x: Int, y: Int)]

    var shapeIndex: Int {
        switch kind {
        case .vertex(let selection):
            selection.shapeIndex
        case .shape(let index):
            index
        }
    }
}

private struct ImagePlaneRasterView: View {
    let plane: ImageExplorerSnapshot.Plane
    let cursor: ImageExplorerSnapshot.PlaneCursor?
    let region: ImageExplorerSnapshot.Region?
    let displayAxes: [ImageExplorerSnapshot.DisplayAxis]
    let probe: ImageExplorerSnapshot.Probe?
    let nonDisplayAxes: [ImageExplorerSnapshot.NonDisplayAxis]
    let regionTool: String
    let regionIsSaved: Bool
    let colorMap: ImageExplorerColorMap
    let onInteraction: (ImagePlaneInteraction) -> Void
    let onClipRangeChange: (Double, Double) -> Void
    @Environment(\.workbenchFontSize) private var workbenchFontSize
    @State private var image: NSImage?
    @State private var draggingClipMarker: ImagePlaneClipMarker?
    @State private var draggingRegion: ImagePlaneRegionDrag?
    @State private var previewClipMin: Double?
    @State private var previewClipMax: Double?
    @State private var pendingBoxStart: (x: Int, y: Int)?
    @State private var hoverPixel: (x: Int, y: Int)?
    @State private var dragPreviewVertices: [(x: Int, y: Int)]?
    @State private var selectedRegionShapeIndex: Int?
    @State private var keyboardFocusNonce = 0

    var body: some View {
        GeometryReader { geometry in
            let layout = ImagePlaneViewportGeometry(
                size: geometry.size,
                plane: plane,
                displayAxes: displayAxes,
                characterSize: workbenchFontSize
            )
            ZStack(alignment: .topLeading) {
                Color(nsColor: .textBackgroundColor)
                if let image {
                    Image(nsImage: image)
                        .resizable()
                        .interpolation(.none)
                        .frame(width: layout.imageRect.width, height: layout.imageRect.height)
                        .position(x: layout.imageRect.midX, y: layout.imageRect.midY)
                }
                Canvas { context, _ in
                    drawAxisAnnotations(in: &context, layout: layout)
                    drawScaleSidebar(in: &context, layout: layout)
                    drawPlaneOverlays(in: &context, layout: layout)
                }
            }
            .clipShape(RoundedRectangle(cornerRadius: 6))
            .overlay(RoundedRectangle(cornerRadius: 6).stroke(Color.secondary.opacity(0.18)))
            .contentShape(Rectangle())
            .gesture(
                DragGesture(minimumDistance: 0)
                    .onChanged { value in
                        if draggingClipMarker == nil && draggingRegion == nil {
                            draggingClipMarker = clipMarker(at: value.startLocation, layout: layout)
                        }
                        if let marker = draggingClipMarker {
                            updatePreviewClip(marker: marker, y: value.location.y, layout: layout)
                        } else if regionTool == "select" {
                            if draggingRegion == nil,
                               let start = sourcePixel(at: value.startLocation, layout: layout) {
                                draggingRegion = regionDrag(at: value.startLocation, startPixel: start, layout: layout)
                                selectedRegionShapeIndex = draggingRegion?.shapeIndex
                                if draggingRegion != nil {
                                    keyboardFocusNonce += 1
                                }
                            }
                            if let draggingRegion,
                               let pixel = sourcePixel(at: value.location, layout: layout) {
                                let moved = pixel.x != draggingRegion.startPixel.x || pixel.y != draggingRegion.startPixel.y
                                dragPreviewVertices = moved ? verticesDragging(draggingRegion, to: pixel) : nil
                            }
                        } else if let pixel = sourcePixel(at: value.location, layout: layout) {
                            hoverPixel = pixel
                        }
                    }
                    .onEnded { value in
                        if let marker = draggingClipMarker {
                            let clip = clipRange(marker: marker, y: value.location.y, layout: layout)
                            previewClipMin = clip.low
                            previewClipMax = clip.high
                            onClipRangeChange(clip.low, clip.high)
                        } else if let preview = dragPreviewVertices, let draggingRegion {
                            commitRegionDrag(draggingRegion, vertices: preview)
                        } else if draggingRegion != nil {
                            keyboardFocusNonce += 1
                        } else if regionTool == "box",
                                  let pixel = sourcePixel(at: value.location, layout: layout) {
                            handleBoxPointerEnd(value: value, pixel: pixel, layout: layout)
                        } else if regionTool == "polygon",
                                  let pixel = sourcePixel(at: value.location, layout: layout) {
                            handlePolygonClick(pixel: pixel, layout: layout)
                        } else if let pixel = sourcePixel(at: value.location, layout: layout) {
                            selectedRegionShapeIndex = nil
                            onInteraction(.selectPixel(pixel.x, pixel.y))
                        }
                        draggingClipMarker = nil
                        draggingRegion = nil
                        dragPreviewVertices = nil
                    }
            )
            .onContinuousHover { phase in
                switch phase {
                case .active(let location):
                    hoverPixel = sourcePixel(at: location, layout: layout)
                case .ended:
                    hoverPixel = nil
                }
            }
            .onExitCommand {
                cancelRegionDrawing()
            }
            .background(
                ImagePlaneKeyCaptureView(focusNonce: keyboardFocusNonce) {
                    deleteSelectedRegionShape()
                }
            )
        }
        .background(Color(nsColor: .textBackgroundColor))
        .onAppear(perform: updateImage)
        .onChange(of: plane.pixelsU8) { _ in updateImage() }
        .onChange(of: colorMap) { _ in updateImage() }
        .onChange(of: plane.clipMin) { _ in clearPreviewClip() }
        .onChange(of: plane.clipMax) { _ in clearPreviewClip() }
        .onChange(of: regionTool) { _ in cancelRegionDrawing() }
    }

    private var displayClipMin: Double {
        previewClipMin ?? plane.clipMin
    }

    private var displayClipMax: Double {
        previewClipMax ?? plane.clipMax
    }

    private func handleBoxPointerEnd(
        value: DragGesture.Value,
        pixel: (x: Int, y: Int),
        layout: ImagePlaneViewportGeometry
    ) {
        let distance = hypot(value.location.x - value.startLocation.x, value.location.y - value.startLocation.y)
        if distance <= 4 {
            handleBoxClick(pixel: pixel)
            return
        }
        if pendingBoxStart == nil {
            pendingBoxStart = sourcePixel(at: value.startLocation, layout: layout)
        }
        hoverPixel = pixel
    }

    private func handleBoxClick(pixel: (x: Int, y: Int)) {
        if let start = pendingBoxStart {
            if start.x != pixel.x || start.y != pixel.y {
                onInteraction(.createRectangle(start.x, start.y, pixel.x, pixel.y))
            }
            pendingBoxStart = nil
            hoverPixel = nil
        } else {
            pendingBoxStart = pixel
            hoverPixel = pixel
        }
    }

    private func handlePolygonClick(pixel: (x: Int, y: Int), layout: ImagePlaneViewportGeometry) {
        if shouldClosePolygon(at: pixel, layout: layout) {
            onInteraction(.closePolygon)
            hoverPixel = nil
            return
        }
        if proposedPolygonWouldIntersect(with: pixel) {
            onInteraction(.rejectRegionEdit("Polygon edges cannot cross."))
            return
        }
        onInteraction(.appendPolygonVertex(pixel.x, pixel.y))
    }

    private func commitRegionDrag(_ drag: ImagePlaneRegionDrag, vertices: [(x: Int, y: Int)]) {
        guard vertices.count >= 3 else { return }
        if polygonSelfIntersects(vertices) {
            onInteraction(.rejectRegionEdit("Polygon edges cannot cross."))
            return
        }
        guard let shapes = regionShapesReplacing(drag: drag, with: vertices) else { return }
        onInteraction(.replaceRegionShapes(shapes))
        selectedRegionShapeIndex = drag.shapeIndex
    }

    private func deleteSelectedRegionShape() -> Bool {
        guard let selectedRegionShapeIndex,
              region?.overlayShapes?.indices.contains(selectedRegionShapeIndex) == true
        else {
            return false
        }
        onInteraction(.deleteRegionShape(selectedRegionShapeIndex))
        self.selectedRegionShapeIndex = nil
        return true
    }

    private func cancelRegionDrawing() {
        pendingBoxStart = nil
        hoverPixel = nil
        dragPreviewVertices = nil
        draggingRegion = nil
    }

    private func drawAxisAnnotations(in context: inout GraphicsContext, layout: ImagePlaneViewportGeometry) {
        guard let xAxis = displayAxes.first, let yAxis = displayAxes[safe: 1] else {
            drawImageFrame(in: &context, rect: layout.imageRect)
            return
        }
        drawImageFrame(in: &context, rect: layout.imageRect)

        let axisColor = Color.secondary.opacity(0.72)
        let tickLength: CGFloat = 6
        let tickFont = Font.system(size: max(10, workbenchFontSize * 0.82))
        let axisFont = Font.system(size: max(11, workbenchFontSize * 0.92), weight: .medium)
        let xTicks = axisTicks(axis: xAxis, length: layout.imageRect.width, reverse: false)
        let yTicks = axisTicks(axis: yAxis, length: layout.imageRect.height, reverse: true)

        for tick in xTicks {
            let x = layout.imageRect.minX + tick.position
            var path = Path()
            path.move(to: CGPoint(x: x, y: layout.imageRect.maxY))
            path.addLine(to: CGPoint(x: x, y: layout.imageRect.maxY + tickLength))
            context.stroke(path, with: .color(axisColor), lineWidth: 1)
            context.draw(
                Text(tick.label).font(tickFont).foregroundColor(.secondary),
                at: CGPoint(x: x, y: layout.imageRect.maxY + tickLength + 5),
                anchor: .top
            )
        }

        for tick in yTicks {
            let y = layout.imageRect.minY + tick.position
            var path = Path()
            path.move(to: CGPoint(x: layout.imageRect.minX - tickLength, y: y))
            path.addLine(to: CGPoint(x: layout.imageRect.minX, y: y))
            context.stroke(path, with: .color(axisColor), lineWidth: 1)
            context.draw(
                Text(tick.label).font(tickFont).foregroundColor(.secondary),
                at: CGPoint(x: layout.imageRect.minX - tickLength - 7, y: y),
                anchor: .trailing
            )
        }

        context.draw(
            Text(axisTitle(xAxis)).font(axisFont).foregroundColor(.secondary),
            at: CGPoint(x: layout.imageRect.midX, y: layout.imageRect.maxY + layout.bottomGutter - 12),
            anchor: .bottom
        )
        let yTickLabelWidth = yTicks
            .map { approximateTextWidth($0.label, fontSize: max(10, workbenchFontSize * 0.82)) }
            .max() ?? 0
        let yAxisTitleX = max(
            12,
            layout.imageRect.minX - tickLength - 7 - yTickLabelWidth - max(22, CGFloat(workbenchFontSize * 1.9))
        )
        var rotated = context
        rotated.translateBy(x: yAxisTitleX, y: layout.imageRect.midY)
        rotated.rotate(by: .degrees(-90))
        rotated.draw(
            Text(axisTitle(yAxis)).font(axisFont).foregroundColor(.secondary),
            at: .zero,
            anchor: .center
        )
    }

    private func drawImageFrame(in context: inout GraphicsContext, rect: CGRect) {
        var path = Path()
        path.addRect(rect)
        context.stroke(path, with: .color(Color.secondary.opacity(0.55)), lineWidth: 1)
    }

    private func drawScaleSidebar(in context: inout GraphicsContext, layout: ImagePlaneViewportGeometry) {
        guard layout.scaleWedgeRect.width >= 8, layout.scaleWedgeRect.height >= 20 else {
            return
        }
        let wedgePath = Path(layout.scaleWedgeRect)
        for offset in 0..<max(Int(layout.scaleWedgeRect.height.rounded(.down)), 1) {
            guard let value = sidebarValue(offset: offset, height: layout.scaleWedgeRect.height) else {
                continue
            }
            let row = CGRect(
                x: layout.scaleWedgeRect.minX,
                y: layout.scaleWedgeRect.minY + CGFloat(offset),
                width: layout.scaleWedgeRect.width,
                height: 1
            )
            context.fill(Path(row), with: .color(sidebarColor(for: value)))
        }
        context.stroke(wedgePath, with: .color(Color.secondary.opacity(0.65)), lineWidth: 1)

        if let histogramRect = layout.histogramRect, let bins = plane.histogramBins, !bins.isEmpty {
            drawHistogram(bins: bins, in: &context, rect: histogramRect)
        }
        drawScaleTicks(in: &context, layout: layout)
        drawScaleMarker(value: displayClipMin, color: .yellow.opacity(0.8), in: &context, layout: layout)
        drawScaleMarker(value: displayClipMax, color: .yellow.opacity(0.8), in: &context, layout: layout)
        if let probe, probe.finite, !probe.masked {
            drawScaleMarker(value: probe.value, color: .cyan.opacity(0.9), in: &context, layout: layout)
        }
    }

    private func drawHistogram(bins: [UInt32], in context: inout GraphicsContext, rect: CGRect) {
        let maxCount = max(bins.max() ?? 0, 1)
        for (index, count) in bins.enumerated() {
            let top = rect.maxY - (CGFloat(index + 1) / CGFloat(bins.count)) * rect.height
            let bottom = rect.maxY - (CGFloat(index) / CGFloat(bins.count)) * rect.height
            let width = rect.width * CGFloat(count) / CGFloat(maxCount)
            guard width > 0 else { continue }
            let bar = CGRect(x: rect.minX, y: top, width: width, height: max(1, bottom - top))
            let value = histogramValue(bin: index, count: bins.count)
            context.fill(Path(bar), with: .color(sidebarColor(for: value).opacity(0.62)))
        }
        context.stroke(Path(rect), with: .color(Color.secondary.opacity(0.45)), lineWidth: 1)
    }

    private func drawScaleTicks(in context: inout GraphicsContext, layout: ImagePlaneViewportGeometry) {
        for value in scaleTicks() {
            guard let y = scaleY(for: value, layout: layout) else { continue }
            var path = Path()
            path.move(to: CGPoint(x: layout.scaleWedgeRect.maxX - 4, y: y))
            path.addLine(to: CGPoint(x: layout.scaleWedgeRect.maxX, y: y))
            context.stroke(path, with: .color(Color.secondary.opacity(0.6)), lineWidth: 1)
            context.draw(
                Text(formatPlaneValue(value, unit: plane.valueUnit))
                    .font(.system(size: max(9, workbenchFontSize * 0.76)))
                    .foregroundColor(.secondary),
                at: CGPoint(x: layout.scaleLabelX, y: y),
                anchor: .leading
            )
        }
        context.draw(
            Text(formatPlaneValueAxisTitle(plane.valueUnit))
                .font(.system(size: max(10, workbenchFontSize * 0.82), weight: .medium))
                .foregroundColor(.secondary),
            at: CGPoint(x: layout.scaleWedgeRect.midX, y: layout.scaleWedgeRect.minY + 5),
            anchor: .top
        )
    }

    private func drawScaleMarker(
        value: Double,
        color: Color,
        in context: inout GraphicsContext,
        layout: ImagePlaneViewportGeometry
    ) {
        guard let y = scaleY(for: value, layout: layout) else { return }
        var path = Path()
        path.move(to: CGPoint(x: layout.scaleWedgeRect.minX, y: y))
        path.addLine(to: CGPoint(x: layout.scaleWedgeRect.maxX, y: y))
        if let histogramRect = layout.histogramRect {
            path.move(to: CGPoint(x: histogramRect.minX, y: y))
            path.addLine(to: CGPoint(x: histogramRect.maxX, y: y))
        }
        context.stroke(path, with: .color(color), lineWidth: 2)
    }

    private func drawPlaneOverlays(in context: inout GraphicsContext, layout: ImagePlaneViewportGeometry) {
        if let region {
            let regionColor = regionIsSaved ? Color.green : Color.yellow
            for (shapeIndex, shape) in (region.overlayShapes ?? []).enumerated() {
                let points = shape.vertices.map { overlayPoint($0.sampledX, $0.sampledY, rect: layout.imageRect) }
                strokeRegionPath(points: points, closed: shape.closed, color: regionColor.opacity(0.85), in: &context)
                if selectedRegionShapeIndex == shapeIndex {
                    strokeRegionPath(points: points, closed: shape.closed, color: Color.white.opacity(0.92), lineWidth: 3.0, in: &context)
                }
                for vertex in shape.vertices {
                    let point = overlayPoint(vertex.sampledX, vertex.sampledY, rect: layout.imageRect)
                    let handle = CGRect(x: point.x - 4, y: point.y - 4, width: 8, height: 8)
                    context.fill(Path(ellipseIn: handle), with: .color(regionColor.opacity(0.95)))
                    context.stroke(Path(ellipseIn: handle), with: .color(Color.black.opacity(0.75)), lineWidth: 1)
                }
            }
        }
        drawRegionInteractionPreview(in: &context, layout: layout)
        if let cursor {
            let point = overlayPoint(Double(cursor.sampledX), Double(cursor.sampledY), rect: layout.imageRect)
            var path = Path()
            path.move(to: CGPoint(x: point.x - 7, y: point.y))
            path.addLine(to: CGPoint(x: point.x + 7, y: point.y))
            path.move(to: CGPoint(x: point.x, y: point.y - 7))
            path.addLine(to: CGPoint(x: point.x, y: point.y + 7))
            context.stroke(path, with: .color(Color.cyan.opacity(0.95)), lineWidth: 1.2)
        }
        drawFrameLabel(in: &context, layout: layout)
    }

    private func drawRegionInteractionPreview(in context: inout GraphicsContext, layout: ImagePlaneViewportGeometry) {
        if let preview = dragPreviewVertices {
            let points = preview.map { overlayPoint(sourcePixel: $0, layout: layout) }
            let color = polygonSelfIntersects(preview) ? Color.red.opacity(0.9) : Color.orange.opacity(0.92)
            strokeRegionPath(points: points, closed: true, color: color, in: &context)
            return
        }
        if regionTool == "box", let start = pendingBoxStart, let hover = hoverPixel {
            let vertices = boxVertices(start: start, end: hover)
            strokeRegionPath(
                points: vertices.map { overlayPoint(sourcePixel: $0, layout: layout) },
                closed: true,
                color: .orange.opacity(0.95),
                in: &context
            )
        } else if regionTool == "polygon",
                  let hover = hoverPixel,
                  let vertices = activeOpenShapeVertices(),
                  let last = vertices.last {
            var path = Path()
            path.move(to: overlayPoint(sourcePixel: last, layout: layout))
            path.addLine(to: overlayPoint(sourcePixel: hover, layout: layout))
            let color = proposedPolygonWouldIntersect(with: hover) ? Color.red.opacity(0.9) : Color.orange.opacity(0.9)
            context.stroke(path, with: .color(color), style: StrokeStyle(lineWidth: 1.5, dash: [5, 4]))
            if vertices.count >= 3, let first = vertices.first {
                let point = overlayPoint(sourcePixel: first, layout: layout)
                let halo = CGRect(x: point.x - 7, y: point.y - 7, width: 14, height: 14)
                context.stroke(Path(ellipseIn: halo), with: .color(Color.orange.opacity(0.95)), lineWidth: 1.5)
            }
        }
    }

    private func strokeRegionPath(
        points: [CGPoint],
        closed: Bool,
        color: Color,
        lineWidth: CGFloat = 1.5,
        in context: inout GraphicsContext
    ) {
        guard let first = points.first else { return }
        var path = Path()
        path.move(to: first)
        for point in points.dropFirst() {
            path.addLine(to: point)
        }
        if closed {
            path.closeSubpath()
        }
        context.stroke(path, with: .color(color), lineWidth: lineWidth)
    }

    private func drawFrameLabel(in context: inout GraphicsContext, layout: ImagePlaneViewportGeometry) {
        let label = nonDisplayAxes
            .map { "\($0.label) \($0.index)/\(max($0.length - 1, 0))" }
            .joined(separator: " | ")
        guard !label.isEmpty, layout.imageRect.width > 80, layout.imageRect.height > 40 else {
            return
        }
        let font = Font.system(size: max(10, workbenchFontSize * 0.82), weight: .medium)
        let badge = CGRect(
            x: layout.imageRect.minX + 8,
            y: layout.imageRect.minY + 8,
            width: min(layout.imageRect.width - 16, CGFloat(label.count) * workbenchFontSize * 0.52 + 18),
            height: max(24, workbenchFontSize + 10)
        )
        let path = Path(roundedRect: badge, cornerRadius: 4)
        context.fill(path, with: .color(Color(nsColor: .textBackgroundColor).opacity(0.82)))
        context.stroke(path, with: .color(Color.secondary.opacity(0.45)), lineWidth: 1)
        context.draw(
            Text(label).font(font).foregroundColor(.primary),
            at: CGPoint(x: badge.minX + 8, y: badge.midY),
            anchor: .leading
        )
    }

    private func overlayPoint(_ x: Double, _ y: Double, rect: CGRect) -> CGPoint {
        CGPoint(
            x: rect.minX + rect.width * CGFloat(x) / CGFloat(max(plane.width - 1, 1)),
            y: rect.minY + rect.height * CGFloat(y) / CGFloat(max(plane.height - 1, 1))
        )
    }

    private func overlayPoint(sourcePixel pixel: (x: Int, y: Int), layout: ImagePlaneViewportGeometry) -> CGPoint {
        let sampled = sampledPixel(sourcePixel: pixel)
        return overlayPoint(Double(sampled.x), Double(sampled.y), rect: layout.imageRect)
    }

    private func sampledPixel(sourcePixel pixel: (x: Int, y: Int)) -> (x: Int, y: Int) {
        guard let xAxis = displayAxes.first, let yAxis = displayAxes[safe: 1] else {
            return pixel
        }
        return (
            x: Int((Double(pixel.x - xAxis.blc) / Double(max(xAxis.inc, 1))).rounded()),
            y: Int((Double(pixel.y - yAxis.blc) / Double(max(yAxis.inc, 1))).rounded())
        )
    }

    private func regionVertex(
        at location: CGPoint,
        layout: ImagePlaneViewportGeometry
    ) -> ImagePlaneVertexSelection? {
        guard let shapes = region?.overlayShapes else { return nil }
        let threshold: CGFloat = 10
        var best: (selection: ImagePlaneVertexSelection, distance: CGFloat)?
        for (shapeIndex, shape) in shapes.enumerated() {
            for (vertexIndex, vertex) in shape.vertices.enumerated() {
                let point = overlayPoint(vertex.sampledX, vertex.sampledY, rect: layout.imageRect)
                let distance = hypot(point.x - location.x, point.y - location.y)
                guard distance <= threshold else { continue }
                if best == nil || distance < best!.distance {
                    best = (ImagePlaneVertexSelection(shapeIndex: shapeIndex, vertexIndex: vertexIndex), distance)
                }
            }
        }
        return best?.selection
    }

    private func regionDrag(
        at location: CGPoint,
        startPixel: (x: Int, y: Int),
        layout: ImagePlaneViewportGeometry
    ) -> ImagePlaneRegionDrag? {
        if let vertex = regionVertex(at: location, layout: layout),
           let vertices = regionShapeVertices(shapeIndex: vertex.shapeIndex) {
            return ImagePlaneRegionDrag(kind: .vertex(vertex), startPixel: startPixel, baseVertices: vertices)
        }
        if let shapeIndex = regionShape(at: location, layout: layout),
           let vertices = regionShapeVertices(shapeIndex: shapeIndex) {
            return ImagePlaneRegionDrag(kind: .shape(shapeIndex), startPixel: startPixel, baseVertices: vertices)
        }
        return nil
    }

    private func verticesDragging(_ drag: ImagePlaneRegionDrag, to pixel: (x: Int, y: Int)) -> [(x: Int, y: Int)] {
        switch drag.kind {
        case .vertex(let selection):
            return drag.baseVertices.enumerated().map { index, vertex in
                clampSourcePixel(index == selection.vertexIndex ? pixel : vertex)
            }
        case .shape:
            let dx = pixel.x - drag.startPixel.x
            let dy = pixel.y - drag.startPixel.y
            return drag.baseVertices.map { vertex in
                clampSourcePixel((x: vertex.x + dx, y: vertex.y + dy))
            }
        }
    }

    private func regionShapeVertices(shapeIndex: Int) -> [(x: Int, y: Int)]? {
        guard let shapes = region?.overlayShapes,
              shapes.indices.contains(shapeIndex)
        else {
            return nil
        }
        var vertices = shapes[shapeIndex].vertices.map { sourcePixel(sampledX: $0.sampledX, sampledY: $0.sampledY) }
        guard vertices.count >= 3 else { return nil }
        vertices = vertices.map(clampSourcePixel)
        return vertices
    }

    private func regionShapesReplacing(
        drag: ImagePlaneRegionDrag,
        with vertices: [(x: Int, y: Int)]
    ) -> [[(x: Int, y: Int)]]? {
        guard let shapes = region?.overlayShapes else { return nil }
        let targetShapeIndex: Int
        switch drag.kind {
        case .vertex(let selection):
            targetShapeIndex = selection.shapeIndex
        case .shape(let shapeIndex):
            targetShapeIndex = shapeIndex
        }
        guard shapes.indices.contains(targetShapeIndex) else { return nil }
        return shapes.enumerated().compactMap { shapeIndex, shape in
            if shapeIndex == targetShapeIndex {
                return vertices.map(clampSourcePixel)
            }
            guard shape.closed, shape.vertices.count >= 3 else { return nil }
            return shape.vertices
                .map { sourcePixel(sampledX: $0.sampledX, sampledY: $0.sampledY) }
                .map(clampSourcePixel)
        }
    }

    private func regionShape(at location: CGPoint, layout: ImagePlaneViewportGeometry) -> Int? {
        guard let shapes = region?.overlayShapes else { return nil }
        for (shapeIndex, shape) in shapes.enumerated().reversed() where shape.closed {
            let points = shape.vertices.map { overlayPoint($0.sampledX, $0.sampledY, rect: layout.imageRect) }
            if pointInPolygon(location, points: points) {
                return shapeIndex
            }
        }
        return nil
    }

    private func activeOpenShapeVertices() -> [(x: Int, y: Int)]? {
        guard let shape = region?.overlayShapes?.last(where: { !$0.closed }) else { return nil }
        return shape.vertices.map { sourcePixel(sampledX: $0.sampledX, sampledY: $0.sampledY) }
    }

    private func shouldClosePolygon(at pixel: (x: Int, y: Int), layout: ImagePlaneViewportGeometry) -> Bool {
        guard let vertices = activeOpenShapeVertices(), vertices.count >= 3, let first = vertices.first else {
            return false
        }
        let firstPoint = overlayPoint(sourcePixel: first, layout: layout)
        let clickPoint = overlayPoint(sourcePixel: pixel, layout: layout)
        return hypot(firstPoint.x - clickPoint.x, firstPoint.y - clickPoint.y) <= 10
    }

    private func proposedPolygonWouldIntersect(with pixel: (x: Int, y: Int)) -> Bool {
        guard let vertices = activeOpenShapeVertices(), vertices.count >= 2 else {
            return false
        }
        let proposed = vertices + [pixel]
        return openPolylineSelfIntersects(proposed)
    }

    private func boxVertices(start: (x: Int, y: Int), end: (x: Int, y: Int)) -> [(x: Int, y: Int)] {
        let x0 = min(start.x, end.x)
        let x1 = max(start.x, end.x)
        let y0 = min(start.y, end.y)
        let y1 = max(start.y, end.y)
        return [(x0, y0), (x1, y0), (x1, y1), (x0, y1)]
    }

    private func clampSourcePixel(_ pixel: (x: Int, y: Int)) -> (x: Int, y: Int) {
        (
            x: min(max(pixel.x, 0), max(plane.width - 1, 0)),
            y: min(max(pixel.y, 0), max(plane.height - 1, 0))
        )
    }

    private func sourcePixel(sampledX: Double, sampledY: Double) -> (x: Int, y: Int) {
        let sampledXInt = Int(sampledX.rounded())
        let sampledYInt = Int(sampledY.rounded())
        guard let xAxis = displayAxes.first, let yAxis = displayAxes[safe: 1] else {
            return (sampledXInt, sampledYInt)
        }
        return (
            xAxis.blc + sampledXInt * max(xAxis.inc, 1),
            yAxis.blc + sampledYInt * max(yAxis.inc, 1)
        )
    }

    private func clipMarker(at location: CGPoint, layout: ImagePlaneViewportGeometry) -> ImagePlaneClipMarker? {
        let wedgeHit = layout.scaleWedgeRect.insetBy(dx: -8, dy: -10)
        let histogramHit = layout.histogramRect?.insetBy(dx: -8, dy: -10)
        guard wedgeHit.contains(location) || histogramHit?.contains(location) == true else {
            return nil
        }
        guard let lowY = scaleY(for: displayClipMin, layout: layout),
              let highY = scaleY(for: displayClipMax, layout: layout)
        else {
            return nil
        }
        let lowDistance = abs(location.y - lowY)
        let highDistance = abs(location.y - highY)
        let threshold = max(10, CGFloat(workbenchFontSize * 0.85))
        guard min(lowDistance, highDistance) <= threshold else {
            return nil
        }
        return lowDistance <= highDistance ? .low : .high
    }

    private func updatePreviewClip(
        marker: ImagePlaneClipMarker,
        y: CGFloat,
        layout: ImagePlaneViewportGeometry
    ) {
        let clip = clipRange(marker: marker, y: y, layout: layout)
        previewClipMin = clip.low
        previewClipMax = clip.high
    }

    private func clipRange(
        marker: ImagePlaneClipMarker,
        y: CGFloat,
        layout: ImagePlaneViewportGeometry
    ) -> (low: Double, high: Double) {
        let dataLow = min(plane.dataMin, plane.dataMax)
        let dataHigh = max(plane.dataMin, plane.dataMax)
        guard dataLow.isFinite, dataHigh.isFinite, dataLow < dataHigh else {
            return (displayClipMin, displayClipMax)
        }
        let minDelta = max((dataHigh - dataLow) * 1e-9, Double.leastNonzeroMagnitude)
        let draggedValue = scaleValue(atY: y, layout: layout).clamped(to: dataLow...dataHigh)
        let currentLow = displayClipMin.isFinite ? displayClipMin : dataLow
        let currentHigh = displayClipMax.isFinite ? displayClipMax : dataHigh
        switch marker {
        case .low:
            return (min(draggedValue, currentHigh - minDelta), currentHigh)
        case .high:
            return (currentLow, max(draggedValue, currentLow + minDelta))
        }
    }

    private func scaleValue(atY y: CGFloat, layout: ImagePlaneViewportGeometry) -> Double {
        let fraction = ((layout.scaleWedgeRect.maxY - y) / max(layout.scaleWedgeRect.height, 1)).clamped(to: 0...1)
        return plane.dataMin + (plane.dataMax - plane.dataMin) * Double(fraction)
    }

    private func clearPreviewClip() {
        previewClipMin = nil
        previewClipMax = nil
    }

    private func sourcePixel(at location: CGPoint, layout: ImagePlaneViewportGeometry) -> (x: Int, y: Int)? {
        guard layout.imageRect.contains(location) else {
            return nil
        }
        guard let xAxis = displayAxes.first, let yAxis = displayAxes[safe: 1] else {
            return (
                imageClickSampleIndex(
                    relative: location.x - layout.imageRect.minX,
                    drawLength: layout.imageRect.width,
                    sampledLength: plane.width
                ),
                imageClickSampleIndex(
                    relative: location.y - layout.imageRect.minY,
                    drawLength: layout.imageRect.height,
                    sampledLength: plane.height
                )
            )
        }
        let sampledX = imageClickSampleIndex(
            relative: location.x - layout.imageRect.minX,
            drawLength: layout.imageRect.width,
            sampledLength: xAxis.sampledLen
        )
        let sampledY = imageClickSampleIndex(
            relative: location.y - layout.imageRect.minY,
            drawLength: layout.imageRect.height,
            sampledLength: yAxis.sampledLen
        )
        return (
            xAxis.blc + sampledX * max(xAxis.inc, 1),
            yAxis.blc + sampledY * max(yAxis.inc, 1)
        )
    }

    private func axisTicks(
        axis: ImageExplorerSnapshot.DisplayAxis,
        length: CGFloat,
        reverse: Bool
    ) -> [ImagePlaneAxisTick] {
        guard axis.sampledLen > 0, length > 0 else {
            return []
        }
        let tickCount = length >= 520 ? 5 : (length >= 300 ? 4 : 3)
        let maxIndex = max(axis.sampledLen - 1, 0)
        var indices = (0..<tickCount).map { step in
            tickCount == 1 ? 0 : Int((Double(step) * Double(maxIndex) / Double(tickCount - 1)).rounded())
        }
        indices = Array(NSOrderedSet(array: indices).compactMap { $0 as? Int })
        return indices.map { sampleIndex in
            let pixel = axis.blc + sampleIndex * max(axis.inc, 1)
            let fraction = maxIndex == 0 ? 0.5 : CGFloat(sampleIndex) / CGFloat(maxIndex)
            let position = reverse ? (1 - fraction) * length : fraction * length
            return ImagePlaneAxisTick(
                label: axisTickLabel(axis: axis, pixel: pixel),
                position: position
            )
        }
    }

    private func axisTickLabel(axis: ImageExplorerSnapshot.DisplayAxis, pixel: Int) -> String {
        guard let value = axisWorldValue(axis: axis, pixel: pixel) else {
            return "\(pixel)"
        }
        if isRightAscensionAxis(axis.name) {
            return formatRightAscension(value)
        }
        if isDeclinationAxis(axis.name) {
            return formatDeclination(value)
        }
        if let frequency = formatFrequencyQuantity(value, unit: axis.unit) {
            return frequency
        }
        return axis.unit.isEmpty ? trimFloatText(value) : "\(trimFloatText(value)) \(axis.unit)"
    }

    private func axisWorldValue(axis: ImageExplorerSnapshot.DisplayAxis, pixel: Int) -> Double? {
        guard let increment = axis.worldIncrement,
              let probe,
              probe.worldAxes.indices.contains(axis.axis),
              probe.pixelIndices.indices.contains(axis.axis) else {
            return nil
        }
        let probeWorld = probe.worldAxes[axis.axis].value
        let probePixel = Double(probe.pixelIndices[axis.axis])
        return probeWorld + (Double(pixel) - probePixel) * increment
    }

    private func axisTitle(_ axis: ImageExplorerSnapshot.DisplayAxis) -> String {
        if isRightAscensionAxis(axis.name) {
            return "Right Ascension"
        }
        if isDeclinationAxis(axis.name) {
            return "Declination"
        }
        return axis.unit.isEmpty ? axis.name : "\(axis.name) [\(axis.unit)]"
    }

    private func scaleTicks() -> [Double] {
        let low = plane.dataMin
        let high = plane.dataMax
        guard low.isFinite, high.isFinite, low != high else {
            return [low]
        }
        return Array((0...4).map { step in
            low + (high - low) * Double(step) / 4.0
        }.reversed())
    }

    private func scaleY(for value: Double, layout: ImagePlaneViewportGeometry) -> CGFloat? {
        let low = plane.dataMin
        let high = plane.dataMax
        guard value.isFinite, low.isFinite, high.isFinite, low != high else {
            return nil
        }
        let fraction = CGFloat((value - low) / (high - low)).clamped(to: 0...1)
        return layout.scaleWedgeRect.maxY - fraction * layout.scaleWedgeRect.height
    }

    private func sidebarValue(offset: Int, height: CGFloat) -> Double? {
        let low = plane.dataMin
        let high = plane.dataMax
        guard low.isFinite, high.isFinite, high != low, height > 1 else {
            return nil
        }
        let fraction = Double(CGFloat(offset) / max(height - 1, 1))
        return high - (high - low) * fraction
    }

    private func histogramValue(bin index: Int, count: Int) -> Double {
        let low = plane.dataMin
        let high = plane.dataMax
        guard low.isFinite, high.isFinite, high != low, count > 1 else {
            return low
        }
        return low + (high - low) * Double(index) / Double(count - 1)
    }

    private func sidebarColor(for value: Double) -> Color {
        let clipLow = displayClipMin
        let clipHigh = displayClipMax
        guard value.isFinite, clipLow.isFinite, clipHigh.isFinite, clipHigh != clipLow else {
            return Color(white: 0)
        }
        let sample = CGFloat((value - clipLow) / (clipHigh - clipLow)).clamped(to: 0...1)
        return imagePlaneColor(sample, colorMap: colorMap)
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
            let color = imagePlaneRGB(value, colorMap: colorMap)
            let offset = index * 4
            data[offset] = color.red
            data[offset + 1] = color.green
            data[offset + 2] = color.blue
            data[offset + 3] = 255
        }
        let output = NSImage(size: NSSize(width: plane.width, height: plane.height))
        if let bitmap {
            output.addRepresentation(bitmap)
        }
        image = output
    }
}

private func pointInPolygon(_ point: CGPoint, points: [CGPoint]) -> Bool {
    guard points.count >= 3 else { return false }
    var inside = false
    var previous = points.count - 1
    for current in points.indices {
        let a = points[current]
        let b = points[previous]
        if ((a.y > point.y) != (b.y > point.y)) {
            let x = (b.x - a.x) * (point.y - a.y) / max(b.y - a.y, CGFloat.leastNonzeroMagnitude) + a.x
            if point.x < x {
                inside.toggle()
            }
        }
        previous = current
    }
    return inside
}

private func openPolylineSelfIntersects(_ vertices: [(x: Int, y: Int)]) -> Bool {
    guard vertices.count >= 4 else { return false }
    let lastSegment = (vertices[vertices.count - 2], vertices[vertices.count - 1])
    for index in 0..<(vertices.count - 3) {
        let segment = (vertices[index], vertices[index + 1])
        if segmentsIntersect(lastSegment.0, lastSegment.1, segment.0, segment.1) {
            return true
        }
    }
    return false
}

private func polygonSelfIntersects(_ vertices: [(x: Int, y: Int)]) -> Bool {
    guard vertices.count >= 4 else { return false }
    for left in vertices.indices {
        let nextLeft = (left + 1) % vertices.count
        for right in (left + 1)..<vertices.count {
            let nextRight = (right + 1) % vertices.count
            if left == right || left == nextRight || nextLeft == right || nextLeft == nextRight {
                continue
            }
            if segmentsIntersect(vertices[left], vertices[nextLeft], vertices[right], vertices[nextRight]) {
                return true
            }
        }
    }
    return false
}

private func segmentsIntersect(
    _ a0: (x: Int, y: Int),
    _ a1: (x: Int, y: Int),
    _ b0: (x: Int, y: Int),
    _ b1: (x: Int, y: Int)
) -> Bool {
    let p1 = CGPoint(x: a0.x, y: a0.y)
    let p2 = CGPoint(x: a1.x, y: a1.y)
    let q1 = CGPoint(x: b0.x, y: b0.y)
    let q2 = CGPoint(x: b1.x, y: b1.y)
    let o1 = orientation(p1, p2, q1)
    let o2 = orientation(p1, p2, q2)
    let o3 = orientation(q1, q2, p1)
    let o4 = orientation(q1, q2, p2)
    if (o1 > 0) != (o2 > 0), (o3 > 0) != (o4 > 0) {
        return true
    }
    return pointOnSegment(q1, p1, p2)
        || pointOnSegment(q2, p1, p2)
        || pointOnSegment(p1, q1, q2)
        || pointOnSegment(p2, q1, q2)
}

private func orientation(_ a: CGPoint, _ b: CGPoint, _ c: CGPoint) -> CGFloat {
    (b.x - a.x) * (c.y - a.y) - (b.y - a.y) * (c.x - a.x)
}

private func pointOnSegment(_ point: CGPoint, _ a: CGPoint, _ b: CGPoint) -> Bool {
    abs(orientation(a, b, point)) < 1.0e-9
        && point.x >= min(a.x, b.x)
        && point.x <= max(a.x, b.x)
        && point.y >= min(a.y, b.y)
        && point.y <= max(a.y, b.y)
}

private struct ImagePlaneAxisTick {
    var label: String
    var position: CGFloat
}

private enum ImagePlaneClipMarker {
    case low
    case high
}

private struct ImagePlaneViewportGeometry {
    var imageRect: CGRect
    var leftGutter: CGFloat
    var rightGutter: CGFloat
    var bottomGutter: CGFloat
    var scaleWedgeRect: CGRect
    var histogramRect: CGRect?
    var scaleLabelX: CGFloat

    init(
        size: CGSize,
        plane: ImageExplorerSnapshot.Plane,
        displayAxes: [ImageExplorerSnapshot.DisplayAxis],
        characterSize: Double
    ) {
        let showAxisAnnotations = displayAxes.count >= 2 && size.width >= 320 && size.height >= 220
        let left = showAxisAnnotations ? Self.leftGutter(characterSize: characterSize) : 12
        let right = size.width >= 300 ? Self.rightGutter(for: size, characterSize: characterSize) : 12
        let top = showAxisAnnotations ? max(26, CGFloat(characterSize * 2.0)) : 12
        let bottom = showAxisAnnotations ? max(56, CGFloat(characterSize * 4.3)) : 12
        let inner = CGRect(
            x: left,
            y: top,
            width: max(1, size.width - left - right),
            height: max(1, size.height - top - bottom)
        )
        let aspect = Self.displayAspectRatio(displayAxes: displayAxes, plane: plane)
        let image = Self.aspectFitRect(aspectRatio: aspect, in: inner)
        let sidebarX = image.maxX + 12
        let availableWidth = max(1, size.width - sidebarX - 8)
        let labelWidth: CGFloat = availableWidth >= 96 ? 56 : (availableWidth >= 76 ? 48 : 36)
        let wedgeWidth = min(max(availableWidth - labelWidth - 12, 12), 18)
        let histogramWidth = max(0, availableWidth - wedgeWidth - labelWidth - 12)
        let wedge = CGRect(x: sidebarX, y: image.minY, width: wedgeWidth, height: image.height)
        let histogram = histogramWidth >= 8
            ? CGRect(x: wedge.maxX + 8, y: image.minY, width: histogramWidth, height: image.height)
            : nil
        imageRect = image
        leftGutter = left
        rightGutter = right
        bottomGutter = bottom
        scaleWedgeRect = wedge
        histogramRect = histogram
        scaleLabelX = (histogram?.maxX ?? wedge.maxX) + 10
    }

    static func leftGutter(characterSize: Double) -> CGFloat {
        max(96, CGFloat(characterSize * 8.0))
    }

    static func rightGutter(for size: CGSize, characterSize _: Double) -> CGFloat {
        min(max(size.width / 6.0, 88), 132)
    }

    static func profileReservedRightGutter(for size: CGSize, characterSize: Double) -> CGFloat {
        max(0, rightGutter(for: size, characterSize: characterSize) - 26)
    }

    private static func aspectFitRect(aspectRatio: CGFloat, in rect: CGRect) -> CGRect {
        guard aspectRatio.isFinite, aspectRatio > 0, rect.width > 0, rect.height > 0 else {
            return rect
        }
        let rectAspect = rect.width / rect.height
        if rectAspect > aspectRatio {
            let width = rect.height * aspectRatio
            return CGRect(x: rect.midX - width / 2, y: rect.minY, width: width, height: rect.height)
        }
        let height = rect.width / aspectRatio
        return CGRect(x: rect.minX, y: rect.midY - height / 2, width: rect.width, height: height)
    }

    private static func displayAspectRatio(
        displayAxes: [ImageExplorerSnapshot.DisplayAxis],
        plane: ImageExplorerSnapshot.Plane
    ) -> CGFloat {
        guard let x = displayAxes.first, let y = displayAxes[safe: 1] else {
            return CGFloat(max(plane.width, 1)) / CGFloat(max(plane.height, 1))
        }
        let xSpan = CGFloat(max(x.trc - x.blc + 1, 1))
        let ySpan = CGFloat(max(y.trc - y.blc + 1, 1))
        let xScale = CGFloat(abs(x.worldIncrement ?? 1)).nonZeroFallback(1)
        let yScale = CGFloat(abs(y.worldIncrement ?? 1)).nonZeroFallback(1)
        return (xSpan * xScale) / (ySpan * yScale)
    }
}

private func imageClickSampleIndex(relative: CGFloat, drawLength: CGFloat, sampledLength: Int) -> Int {
    guard drawLength > 0, sampledLength > 0 else {
        return 0
    }
    let numerator = (max(0, relative) * 2 + 1) * CGFloat(sampledLength)
    let denominator = drawLength * 2
    return min(max(Int(numerator / denominator), 0), sampledLength - 1)
}

private func isRightAscensionAxis(_ name: String) -> Bool {
    name.compare("Right Ascension", options: .caseInsensitive) == .orderedSame
        || name.compare("RA", options: .caseInsensitive) == .orderedSame
}

private func isDeclinationAxis(_ name: String) -> Bool {
    name.compare("Declination", options: .caseInsensitive) == .orderedSame
        || name.compare("DEC", options: .caseInsensitive) == .orderedSame
}

private func formatRightAscension(_ radians: Double) -> String {
    var totalSeconds = radians * 86_400.0 / (Double.pi * 2)
    totalSeconds.formTruncatingRemainder(dividingBy: 86_400)
    if totalSeconds < 0 {
        totalSeconds += 86_400
    }
    let hours = Int(totalSeconds / 3_600)
    let minutes = Int((totalSeconds - Double(hours * 3_600)) / 60)
    let seconds = totalSeconds - Double(hours * 3_600 + minutes * 60)
    return String(format: "%02d:%02d:%05.2f", hours, minutes, seconds)
}

private func formatDeclination(_ radians: Double) -> String {
    let degrees = radians * 180.0 / Double.pi
    let sign = degrees < 0 ? "-" : "+"
    let absDegrees = abs(degrees)
    let wholeDegrees = Int(absDegrees)
    let minutesFloat = (absDegrees - Double(wholeDegrees)) * 60
    let minutes = Int(minutesFloat)
    let seconds = (minutesFloat - Double(minutes)) * 60
    return String(format: "%@%02d:%02d:%04.1f", sign, wholeDegrees, minutes, seconds)
}

private func formatFrequencyQuantity(_ value: Double, unit: String) -> String? {
    guard unit.compare("Hz", options: .caseInsensitive) == .orderedSame else {
        return nil
    }
    let absValue = abs(value)
    let scale: Double
    let suffix: String
    if absValue >= 1e9 {
        scale = 1e9
        suffix = "GHz"
    } else if absValue >= 1e6 {
        scale = 1e6
        suffix = "MHz"
    } else if absValue >= 1e3 {
        scale = 1e3
        suffix = "kHz"
    } else {
        scale = 1
        suffix = "Hz"
    }
    return "\(trimFloatText(value / scale)) \(suffix)"
}

private func trimFloatText(_ value: Double) -> String {
    let text = String(format: "%.4g", value)
    return text.replacingOccurrences(of: "+0", with: "+")
}

private func formatPlaneValue(_ value: Double, unit: String) -> String {
    unit.isEmpty ? trimFloatText(value) : "\(trimFloatText(value)) \(unit)"
}

private func formatPlaneValueAxisTitle(_ unit: String) -> String {
    unit.isEmpty ? "Value" : "Value [\(unit)]"
}

private func imagePlaneColor(_ sample: CGFloat, colorMap: ImageExplorerColorMap) -> Color {
    let value = UInt8((sample.clamped(to: 0...1) * 255).rounded())
    let rgb = imagePlaneRGB(value, colorMap: colorMap)
    return Color(
        red: Double(rgb.red) / 255.0,
        green: Double(rgb.green) / 255.0,
        blue: Double(rgb.blue) / 255.0
    )
}

private func imagePlaneRGB(
    _ value: UInt8,
    colorMap: ImageExplorerColorMap
) -> (red: UInt8, green: UInt8, blue: UInt8) {
    switch colorMap {
    case .grayscale:
        return (value, value, value)
    case .viridis:
        return interpolateColorStops(
            value,
            stops: [(68, 1, 84), (59, 82, 139), (33, 145, 140), (94, 201, 98), (253, 231, 37)]
        )
    case .inferno:
        return interpolateColorStops(
            value,
            stops: [(0, 0, 4), (87, 15, 109), (187, 55, 84), (249, 142, 8), (252, 255, 164)]
        )
    case .magma:
        return interpolateColorStops(
            value,
            stops: [(0, 0, 4), (74, 16, 107), (179, 53, 88), (251, 135, 97), (252, 253, 191)]
        )
    case .coolWarm:
        return interpolateColorStops(
            value,
            stops: [(59, 76, 192), (180, 205, 232), (245, 245, 245), (221, 132, 105), (180, 4, 38)]
        )
    }
}

private func interpolateColorStops(
    _ value: UInt8,
    stops: [(red: UInt8, green: UInt8, blue: UInt8)]
) -> (red: UInt8, green: UInt8, blue: UInt8) {
    guard !stops.isEmpty else {
        return (value, value, value)
    }
    guard stops.count > 1 else {
        return stops[0]
    }
    let segmentCount = stops.count - 1
    let scaled = Int(value) * segmentCount * 256 / 255
    let segment = min(scaled / 256, segmentCount - 1)
    let fraction = scaled % 256
    let start = stops[segment]
    let end = stops[segment + 1]
    return (
        interpolateChannel(start.red, end.red, fraction: fraction),
        interpolateChannel(start.green, end.green, fraction: fraction),
        interpolateChannel(start.blue, end.blue, fraction: fraction)
    )
}

private func interpolateChannel(_ start: UInt8, _ end: UInt8, fraction: Int) -> UInt8 {
    let startValue = Int(start)
    let delta = Int(end) - startValue
    return UInt8(clamping: startValue + (delta * fraction + 128) / 256)
}

private func approximateTextWidth(_ text: String, fontSize: Double) -> CGFloat {
    CGFloat(text.count) * CGFloat(fontSize) * 0.58
}

private extension CGFloat {
    func clamped(to range: ClosedRange<CGFloat>) -> CGFloat {
        Swift.min(Swift.max(self, range.lowerBound), range.upperBound)
    }

    func nonZeroFallback(_ fallback: CGFloat) -> CGFloat {
        self > 0 ? self : fallback
    }
}

private extension Double {
    func clamped(to range: ClosedRange<Double>) -> Double {
        Swift.min(Swift.max(self, range.lowerBound), range.upperBound)
    }
}

private extension Array {
    subscript(safe index: Int) -> Element? {
        guard indices.contains(index) else { return nil }
        return self[index]
    }
}

func tableBrowserAddressSummary(_ address: TableBrowserSnapshot.SelectedAddress?) -> String? {
    guard let address else {
        return nil
    }
    switch address.kind {
    case "column":
        return address.column.map { "column \($0)" }
    case "cell":
        let row = address.row.map(String.init) ?? "?"
        return "row \(row) \(address.column ?? "")"
    case "table_keyword":
        return "keyword \(address.keywordPath?.joined(separator: ".") ?? "")"
    case "column_keyword":
        return "keyword \(address.column ?? ""):\(address.keywordPath?.joined(separator: ".") ?? "")"
    case "subtable":
        return "subtable \(address.targetPath ?? "")"
    default:
        return address.kind
    }
}

private struct TableBrowserSnapshotView: View {
    let snapshot: TableBrowserSnapshot
    let cellWindow: TableBrowserCellWindowSnapshot?
    let hiddenColumns: Set<Int>
    let arrayInlineLimits: [Int: Int]
    let interfaceFontSize: Double
    let selectedCellRow: Int?
    let selectedCellColumn: Int?
    let selectMainItem: (Int) -> Void
    let selectCell: (_ rowIndex: Int?, _ selectedVisibleColumn: Int?, _ targetVisibleColumn: Int?) -> Void
    let requestCellWindow: (_ rowStart: Int, _ rowLimit: Int, _ columnStart: Int, _ columnLimit: Int) -> Void
    let setColumnHidden: (_ columnIndex: Int, _ hidden: Bool) -> Void
    let setArrayInlineLimit: (_ columnIndex: Int, _ limit: Int) -> Void
    let copyCellValue: (_ rowIndex: Int, _ columnIndex: Int) -> Void
    let loadCellValue: (_ rowIndex: Int, _ columnIndex: Int, _ completion: @escaping (String?) -> Void) -> Void
    let openSelectedSubtable: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack(spacing: 10) {
                if snapshot.capabilities?.editable == false {
                    Text("read-only")
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                metricsLabel(snapshot.verticalMetrics, axis: "rows")
                metricsLabel(snapshot.horizontalMetrics, axis: "cols")
            }
            .padding(.horizontal, 12)
            .padding(.top, 8)

            TableBrowserMainPane(
                snapshot: snapshot,
                cellWindow: cellWindow,
                hiddenColumns: hiddenColumns,
                arrayInlineLimits: arrayInlineLimits,
                interfaceFontSize: interfaceFontSize,
                selectedCellRow: selectedCellRow,
                selectedCellColumn: selectedCellColumn,
                selectMainItem: selectMainItem,
                selectCell: selectCell,
                requestCellWindow: requestCellWindow,
                setColumnHidden: setColumnHidden,
                setArrayInlineLimit: setArrayInlineLimit,
                copyCellValue: copyCellValue,
                loadCellValue: loadCellValue,
                openSelectedSubtable: openSelectedSubtable
            )
            .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .accessibilityIdentifier("tableBrowser.snapshot")
    }

    @ViewBuilder
    private func metricsLabel(_ metrics: TableBrowserSnapshot.NavigationMetrics?, axis: String) -> some View {
        if let metrics, metrics.totalItems > 0 {
            Text("\(axis) \(metrics.selectedIndex + 1)/\(metrics.totalItems)")
                .workbenchFont(.caption, design: .monospaced)
                .foregroundStyle(.secondary)
        }
    }
}

private struct TableBrowserMainPane: View {
    let snapshot: TableBrowserSnapshot
    let cellWindow: TableBrowserCellWindowSnapshot?
    let hiddenColumns: Set<Int>
    let arrayInlineLimits: [Int: Int]
    let interfaceFontSize: Double
    let selectedCellRow: Int?
    let selectedCellColumn: Int?
    let selectMainItem: (Int) -> Void
    let selectCell: (_ rowIndex: Int?, _ selectedVisibleColumn: Int?, _ targetVisibleColumn: Int?) -> Void
    let requestCellWindow: (_ rowStart: Int, _ rowLimit: Int, _ columnStart: Int, _ columnLimit: Int) -> Void
    let setColumnHidden: (_ columnIndex: Int, _ hidden: Bool) -> Void
    let setArrayInlineLimit: (_ columnIndex: Int, _ limit: Int) -> Void
    let copyCellValue: (_ rowIndex: Int, _ columnIndex: Int) -> Void
    let loadCellValue: (_ rowIndex: Int, _ columnIndex: Int, _ completion: @escaping (String?) -> Void) -> Void
    let openSelectedSubtable: () -> Void
    @State private var inspectedColumn: TableBrowserCellWindowSnapshot.Column?
    @State private var inspectedCell: TableBrowserCellInspectorItem?

    var body: some View {
        Group {
            switch snapshot.view {
            case "keywords":
                TableBrowserKeyValueGrid(
                    lines: tableKeywordLines,
                    selectedIndex: snapshot.verticalMetrics?.selectedIndex,
                    selectMainItem: selectMainItem
                )
            case "subtables":
                TableBrowserSubtableGrid(
                    lines: mainLines,
                    selectMainItem: selectMainItem,
                    openSelectedSubtable: openSelectedSubtable
                )
            default:
                if let cellWindow {
                    VStack(alignment: .leading, spacing: 0) {
                        TableBrowserCellsToolbar(
                            grid: cellWindow,
                            hiddenColumns: hiddenColumns,
                            setColumnHidden: setColumnHidden
                        )
                        Divider()
                        TableBrowserNativeCellsGrid(
                            grid: cellWindow,
                            hiddenColumns: hiddenColumns,
                            arrayInlineLimits: arrayInlineLimits,
                            interfaceFontSize: interfaceFontSize,
                            selectedRow: selectedCellRow ?? snapshot.verticalMetrics?.selectedIndex,
                            selectedColumn: selectedCellColumn ?? snapshot.horizontalMetrics?.selectedIndex,
                            selectCell: selectCell,
                            requestCellWindow: requestCellWindow,
                            setColumnHidden: setColumnHidden,
                            setArrayInlineLimit: setArrayInlineLimit,
                            inspectColumn: { inspectedColumn = $0 },
                            inspectCell: { inspectedCell = $0 },
                            copyCellValue: copyCellValue,
                            loadCellValue: loadCellValue
                        )
                    }
                    .sheet(item: $inspectedColumn) { column in
                        TableBrowserColumnInspector(column: column)
                    }
                    .sheet(item: $inspectedCell) { cell in
                        TableBrowserCellInspector(item: cell)
                    }
                } else {
                    TableBrowserCellsGrid(
                        table: TableBrowserRenderedCellTable(lines: mainLines),
                        selectCell: selectCell
                    )
                }
            }
        }
        .background(Color(nsColor: .windowBackgroundColor))
        .overlay(Rectangle().stroke(Color.secondary.opacity(0.20), lineWidth: 0.5))
    }

    private var mainLines: [String] {
        var lines = snapshot.contentLines
        guard let inspector = snapshot.inspector?.renderedLines, !inspector.isEmpty, lines.count >= inspector.count else {
            return lines
        }
        if Array(lines.suffix(inspector.count)) == inspector {
            lines.removeLast(inspector.count)
            if lines.last == "" {
                lines.removeLast()
            }
        }
        return lines
    }

    private var tableKeywordLines: [String] {
        let lines = mainLines
        guard !lines.isEmpty else {
            return lines
        }
        var filtered = [lines[0].replacingOccurrences(of: "Keywords", with: "Table Keywords")]
        filtered += lines.dropFirst().filter { line in
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            return trimmed.hasPrefix("table.")
                || trimmed.hasPrefix("> table.")
        }
        if filtered.count == 1 {
            filtered.append(" No table keywords")
        }
        return filtered
    }
}

private struct TableBrowserRenderedCellTable {
    var headers: [String]
    var rows: [TableBrowserRenderedCellRow]

    init(lines: [String]) {
        var headers: [String] = []
        var rows: [TableBrowserRenderedCellRow] = []
        for line in lines {
            if line.trimmingCharacters(in: .whitespaces).hasPrefix("Cells ") {
                continue
            }
            guard line.contains("|") else {
                continue
            }
            let fields = TableBrowserRenderedCellTable.splitFields(line)
            guard !fields.isEmpty else {
                continue
            }
            if fields[0].trimmingCharacters(in: .whitespaces).lowercased() == "row" {
                headers = fields
                continue
            }
            if let row = TableBrowserRenderedCellRow(fields: fields, rawLine: line) {
                rows.append(row)
            }
        }
        self.headers = headers
        self.rows = rows
    }

    private static func splitFields(_ line: String) -> [String] {
        line
            .split(separator: "|", omittingEmptySubsequences: false)
            .map { $0.trimmingCharacters(in: .whitespaces) }
            .filter { !$0.isEmpty }
    }
}

private struct TableBrowserRenderedCellRow: Identifiable, Equatable {
    var id: Int { rowIndex ?? fallbackID }
    var rowIndex: Int?
    var fallbackID: Int
    var selectedRow: Bool
    var cells: [TableBrowserRenderedCell]

    init?(fields: [String], rawLine: String) {
        guard !fields.isEmpty else {
            return nil
        }
        let rawRow = fields[0]
            .replacingOccurrences(of: ">", with: "")
            .trimmingCharacters(in: .whitespaces)
        rowIndex = Int(rawRow)
        fallbackID = rawLine.hashValue
        selectedRow = rawLine.first == ">"
        cells = fields.dropFirst().map { TableBrowserRenderedCell(raw: $0) }
    }
}

private struct TableBrowserRenderedCell: Equatable {
    var text: String
    var selected: Bool

    init(raw: String) {
        let trimmed = raw.trimmingCharacters(in: .whitespaces)
        selected = trimmed.hasPrefix(">") && trimmed.contains("<")
        text = trimmed
            .replacingOccurrences(of: ">", with: "")
            .replacingOccurrences(of: "<", with: "")
    }
}

extension TableBrowserCellWindowSnapshot.Column: Identifiable {
    public var id: Int { index }
}

private struct TableBrowserCellsToolbar: View {
    let grid: TableBrowserCellWindowSnapshot
    let hiddenColumns: Set<Int>
    let setColumnHidden: (_ columnIndex: Int, _ hidden: Bool) -> Void

    var body: some View {
        HStack(spacing: 8) {
            Menu {
                Button("Show All Columns") {
                    for columnIndex in hiddenColumns {
                        setColumnHidden(columnIndex, false)
                    }
                }
                .disabled(hiddenColumns.isEmpty)
                Divider()
                ForEach(grid.columns) { column in
                    Button {
                        setColumnHidden(column.index, !hiddenColumns.contains(column.index))
                    } label: {
                        Label(
                            column.name,
                            systemImage: hiddenColumns.contains(column.index) ? "square" : "checkmark.square"
                        )
                    }
                }
                let hiddenOutsideWindow = hiddenColumns.subtracting(Set(grid.columns.map(\.index)))
                if !hiddenOutsideWindow.isEmpty {
                    Divider()
                    ForEach(hiddenOutsideWindow.sorted(), id: \.self) { columnIndex in
                        Button("Show Column \(columnIndex)") {
                            setColumnHidden(columnIndex, false)
                        }
                    }
                }
            } label: {
                Label("Columns", systemImage: "tablecolumns")
            }

            if !hiddenColumns.isEmpty {
                Text("\(hiddenColumns.count) hidden")
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }
            Spacer()
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 5)
        .background(Color(nsColor: .controlBackgroundColor))
    }
}

private struct TableBrowserColumnInspector: View {
    let column: TableBrowserCellWindowSnapshot.Column
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                VStack(alignment: .leading, spacing: 4) {
                    Text(column.name)
                        .workbenchFont(.headline, weight: .semibold)
                    Text(column.summary)
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                Button("Done") {
                    dismiss()
                }
            }
            Divider()
            if column.keywords.isEmpty {
                Text("No column keywords")
                    .foregroundStyle(.secondary)
            } else {
                ScrollView {
                    VStack(alignment: .leading, spacing: 4) {
                        ForEach(column.keywords, id: \.self) { keyword in
                            Text(keyword)
                                .workbenchFont(.caption, design: .monospaced)
                                .textSelection(.enabled)
                                .frame(maxWidth: .infinity, alignment: .leading)
                        }
                    }
                }
            }
        }
        .padding(16)
        .frame(width: 520, height: 360)
    }
}

private struct TableBrowserCellInspectorItem: Identifiable {
    let rowIndex: Int
    let column: TableBrowserCellWindowSnapshot.Column
    let value: String

    var id: String {
        "\(rowIndex):\(column.index):\(value.hashValue)"
    }
}

private struct TableBrowserCellInspector: View {
    let item: TableBrowserCellInspectorItem
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                VStack(alignment: .leading, spacing: 4) {
                    Text("\(item.column.name) row \(item.rowIndex)")
                        .workbenchFont(.headline, weight: .semibold)
                    Text(item.column.summary)
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                Button("Done") {
                    dismiss()
                }
            }
            Divider()
            ScrollView {
                Text(item.value)
                    .workbenchFont(.body, design: .monospaced)
                    .textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding(8)
            }
            .background(Color(nsColor: .textBackgroundColor))
            .clipShape(RoundedRectangle(cornerRadius: 6))
        }
        .padding(16)
        .frame(width: 620, height: 360)
    }
}

private final class TableBrowserHeaderView: NSTableHeaderView {
    var menuProvider: ((NSTableHeaderView, NSEvent) -> NSMenu?)?

    override func rightMouseDown(with event: NSEvent) {
        if let menu = menuProvider?(self, event) {
            NSMenu.popUpContextMenu(menu, with: event, for: self)
        } else {
            super.rightMouseDown(with: event)
        }
    }
}

private final class TableBrowserTableView: NSTableView {
    var menuProvider: ((TableBrowserTableView, NSEvent) -> NSMenu?)?
    var copyHandler: (() -> Void)?

    override func menu(for event: NSEvent) -> NSMenu? {
        menuProvider?(self, event) ?? super.menu(for: event)
    }

    @objc func copy(_ sender: Any?) {
        copyHandler?()
    }

    override func performKeyEquivalent(with event: NSEvent) -> Bool {
        if event.modifierFlags.intersection(.deviceIndependentFlagsMask).contains(.command),
           event.charactersIgnoringModifiers?.lowercased() == "c" {
            copyHandler?()
            return true
        }
        return super.performKeyEquivalent(with: event)
    }
}

private struct TableBrowserNativeCellsGrid: NSViewRepresentable {
    let grid: TableBrowserCellWindowSnapshot
    let hiddenColumns: Set<Int>
    let arrayInlineLimits: [Int: Int]
    let interfaceFontSize: Double
    let selectedRow: Int?
    let selectedColumn: Int?
    let selectCell: (_ rowIndex: Int?, _ selectedVisibleColumn: Int?, _ targetVisibleColumn: Int?) -> Void
    let requestCellWindow: (_ rowStart: Int, _ rowLimit: Int, _ columnStart: Int, _ columnLimit: Int) -> Void
    let setColumnHidden: (_ columnIndex: Int, _ hidden: Bool) -> Void
    let setArrayInlineLimit: (_ columnIndex: Int, _ limit: Int) -> Void
    let inspectColumn: (TableBrowserCellWindowSnapshot.Column) -> Void
    let inspectCell: (TableBrowserCellInspectorItem) -> Void
    let copyCellValue: (_ rowIndex: Int, _ columnIndex: Int) -> Void
    let loadCellValue: (_ rowIndex: Int, _ columnIndex: Int, _ completion: @escaping (String?) -> Void) -> Void

    func makeCoordinator() -> Coordinator {
        Coordinator(
            grid: grid,
            hiddenColumns: hiddenColumns,
            arrayInlineLimits: arrayInlineLimits,
            interfaceFontSize: interfaceFontSize,
            selectedRow: selectedRow,
            selectedColumn: selectedColumn,
            selectCell: selectCell,
            requestCellWindow: requestCellWindow,
            setColumnHidden: setColumnHidden,
            setArrayInlineLimit: setArrayInlineLimit,
            inspectColumn: inspectColumn,
            inspectCell: inspectCell,
            copyCellValue: copyCellValue,
            loadCellValue: loadCellValue
        )
    }

    func makeNSView(context: Context) -> NSScrollView {
        let scrollView = NSScrollView()
        scrollView.hasVerticalScroller = true
        scrollView.hasHorizontalScroller = true
        scrollView.autohidesScrollers = false
        scrollView.scrollerStyle = .legacy
        scrollView.borderType = .noBorder
        scrollView.drawsBackground = true
        scrollView.backgroundColor = .windowBackgroundColor

        let tableView = TableBrowserTableView()
        let headerView = TableBrowserHeaderView()
        headerView.menuProvider = { [weak coordinator = context.coordinator] headerView, event in
            coordinator?.headerMenu(for: headerView, event: event)
        }
        tableView.headerView = headerView
        tableView.usesAlternatingRowBackgroundColors = false
        tableView.gridStyleMask = [.solidHorizontalGridLineMask, .solidVerticalGridLineMask]
        tableView.rowHeight = max(22, CGFloat(interfaceFontSize) + 8)
        tableView.allowsColumnResizing = true
        tableView.allowsColumnReordering = false
        tableView.allowsMultipleSelection = false
        tableView.allowsEmptySelection = true
        tableView.selectionHighlightStyle = .none
        tableView.columnAutoresizingStyle = .noColumnAutoresizing
        tableView.dataSource = context.coordinator
        tableView.delegate = context.coordinator
        tableView.target = context.coordinator
        tableView.action = #selector(Coordinator.tableCellClicked(_:))
        tableView.doubleAction = #selector(Coordinator.tableCellDoubleClicked(_:))
        tableView.menuProvider = { [weak coordinator = context.coordinator] tableView, event in
            coordinator?.cellMenu(for: tableView, event: event)
        }
        tableView.copyHandler = { [weak coordinator = context.coordinator] in
            coordinator?.copySelectedCell()
        }
        scrollView.documentView = tableView

        context.coordinator.tableView = tableView
        context.coordinator.attach(to: scrollView)
        context.coordinator.syncColumns()
        context.coordinator.reloadDataIfNeeded()
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        context.coordinator.grid = grid
        context.coordinator.hiddenColumns = hiddenColumns
        context.coordinator.arrayInlineLimits = arrayInlineLimits
        context.coordinator.interfaceFontSize = interfaceFontSize
        context.coordinator.selectedRow = selectedRow
        context.coordinator.selectedColumn = selectedColumn
        context.coordinator.selectCell = selectCell
        context.coordinator.requestCellWindow = requestCellWindow
        context.coordinator.setColumnHidden = setColumnHidden
        context.coordinator.setArrayInlineLimit = setArrayInlineLimit
        context.coordinator.inspectColumn = inspectColumn
        context.coordinator.inspectCell = inspectCell
        context.coordinator.copyCellValue = copyCellValue
        context.coordinator.loadCellValue = loadCellValue
        context.coordinator.syncLocalSelectionFromState()
        context.coordinator.tableView?.rowHeight = max(22, CGFloat(interfaceFontSize) + 8)
        context.coordinator.syncColumns()
        context.coordinator.reloadDataIfNeeded()
        context.coordinator.restoreSelection()
        context.coordinator.requestVisibleWindowIfNeeded()
    }

    static func dismantleNSView(_ scrollView: NSScrollView, coordinator: Coordinator) {
        coordinator.detach()
    }

    final class Coordinator: NSObject, NSTableViewDataSource, NSTableViewDelegate {
        var grid: TableBrowserCellWindowSnapshot
        var hiddenColumns: Set<Int>
        var arrayInlineLimits: [Int: Int]
        var interfaceFontSize: Double
        var selectedRow: Int?
        var selectedColumn: Int?
        var selectCell: (_ rowIndex: Int?, _ selectedVisibleColumn: Int?, _ targetVisibleColumn: Int?) -> Void
        var requestCellWindow: (_ rowStart: Int, _ rowLimit: Int, _ columnStart: Int, _ columnLimit: Int) -> Void
        var setColumnHidden: (_ columnIndex: Int, _ hidden: Bool) -> Void
        var setArrayInlineLimit: (_ columnIndex: Int, _ limit: Int) -> Void
        var inspectColumn: (TableBrowserCellWindowSnapshot.Column) -> Void
        var inspectCell: (TableBrowserCellInspectorItem) -> Void
        var copyCellValue: (_ rowIndex: Int, _ columnIndex: Int) -> Void
        var loadCellValue: (_ rowIndex: Int, _ columnIndex: Int, _ completion: @escaping (String?) -> Void) -> Void
        weak var tableView: NSTableView?
        private weak var scrollView: NSScrollView?
        private var lastColumnIDs: [String] = []
        private var lastRequestedWindow: String?
        private var pendingRequestWorkItem: DispatchWorkItem?
        private var lastReloadSignature: String?
        private var cellValues: [String: String] = [:]
        private var localSelectedRow: Int?
        private var localSelectedColumn: Int?
        private let cellIdentifier = NSUserInterfaceItemIdentifier("TableBrowserCell")

        init(
            grid: TableBrowserCellWindowSnapshot,
            hiddenColumns: Set<Int>,
            arrayInlineLimits: [Int: Int],
            interfaceFontSize: Double,
            selectedRow: Int?,
            selectedColumn: Int?,
            selectCell: @escaping (_ rowIndex: Int?, _ selectedVisibleColumn: Int?, _ targetVisibleColumn: Int?) -> Void,
            requestCellWindow: @escaping (_ rowStart: Int, _ rowLimit: Int, _ columnStart: Int, _ columnLimit: Int) -> Void,
            setColumnHidden: @escaping (_ columnIndex: Int, _ hidden: Bool) -> Void,
            setArrayInlineLimit: @escaping (_ columnIndex: Int, _ limit: Int) -> Void,
            inspectColumn: @escaping (TableBrowserCellWindowSnapshot.Column) -> Void,
            inspectCell: @escaping (TableBrowserCellInspectorItem) -> Void,
            copyCellValue: @escaping (_ rowIndex: Int, _ columnIndex: Int) -> Void,
            loadCellValue: @escaping (_ rowIndex: Int, _ columnIndex: Int, _ completion: @escaping (String?) -> Void) -> Void
        ) {
            self.grid = grid
            self.hiddenColumns = hiddenColumns
            self.arrayInlineLimits = arrayInlineLimits
            self.interfaceFontSize = interfaceFontSize
            self.selectedRow = selectedRow
            self.selectedColumn = selectedColumn
            self.selectCell = selectCell
            self.requestCellWindow = requestCellWindow
            self.setColumnHidden = setColumnHidden
            self.setArrayInlineLimit = setArrayInlineLimit
            self.inspectColumn = inspectColumn
            self.inspectCell = inspectCell
            self.copyCellValue = copyCellValue
            self.loadCellValue = loadCellValue
            self.localSelectedRow = selectedRow
            self.localSelectedColumn = selectedColumn
        }

        func attach(to scrollView: NSScrollView) {
            detach()
            self.scrollView = scrollView
            scrollView.contentView.postsBoundsChangedNotifications = true
            NotificationCenter.default.addObserver(
                self,
                selector: #selector(boundsDidChange(_:)),
                name: NSView.boundsDidChangeNotification,
                object: scrollView.contentView
            )
        }

        func detach() {
            pendingRequestWorkItem?.cancel()
            pendingRequestWorkItem = nil
            if let scrollView {
                NotificationCenter.default.removeObserver(
                    self,
                    name: NSView.boundsDidChangeNotification,
                    object: scrollView.contentView
                )
            }
            scrollView = nil
        }

        func reloadDataIfNeeded() {
            let signature = [
                grid.tablePath,
                "\(grid.rowCount)",
                "\(grid.columnCount)",
                "\(grid.rowStart)",
                "\(grid.rows.count)",
                "\(grid.columnStart)",
                "\(grid.rows.first?.cells.count ?? 0)",
                hiddenColumns.sorted().map(String.init).joined(separator: ","),
                arrayInlineLimits.sorted { $0.key < $1.key }.map { "\($0.key)=\($0.value)" }.joined(separator: ","),
                grid.rows.flatMap { $0.cells }.map { "\($0.columnIndex)=\($0.display.count)" }.joined(separator: ","),
                "\(interfaceFontSize)"
            ].joined(separator: ":")
            guard signature != lastReloadSignature else {
                return
            }
            lastReloadSignature = signature
            rebuildCellLookup()
            tableView?.reloadData()
        }

        private func rebuildCellLookup() {
            var values: [String: String] = [:]
            values.reserveCapacity(grid.rows.reduce(0) { $0 + $1.cells.count })
            for row in grid.rows {
                for cell in row.cells {
                    values["\(row.index):\(cell.columnIndex)"] = cell.display
                }
            }
            cellValues = values
        }

        func syncColumns() {
            guard let tableView else {
                return
            }
            let displayedColumns = grid.columns.filter { !hiddenColumns.contains($0.index) }
            let desiredIDs = ["row"] + displayedColumns.map { "column-\($0.index)" }
            guard desiredIDs != lastColumnIDs else {
                updateColumnMetrics(tableView)
                return
            }
            for column in tableView.tableColumns {
                tableView.removeTableColumn(column)
            }

            let rowColumn = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("row"))
            rowColumn.title = "Row"
            rowColumn.width = 72
            rowColumn.minWidth = 56
            rowColumn.maxWidth = 120
            tableView.addTableColumn(rowColumn)

            for column in displayedColumns {
                let tableColumn = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("column-\(column.index)"))
                tableColumn.title = column.header
                tableColumn.width = columnWidth(for: column)
                tableColumn.minWidth = 72
                tableColumn.maxWidth = arrayInlineLimits[column.index, default: 0] > 0 ? 900 : 420
                tableColumn.headerToolTip = "\(column.name): \(column.summary)"
                tableView.addTableColumn(tableColumn)
            }
            lastColumnIDs = desiredIDs
        }

        private func updateColumnMetrics(_ tableView: NSTableView) {
            for column in grid.columns {
                guard let tableColumn = tableView.tableColumns.first(where: { $0.identifier.rawValue == "column-\(column.index)" }) else {
                    continue
                }
                tableColumn.title = column.header
                tableColumn.headerToolTip = "\(column.name): \(column.summary)"
                tableColumn.width = min(max(tableColumn.width, columnWidth(for: column)), tableColumn.maxWidth)
            }
        }

        private func columnWidth(for column: TableBrowserCellWindowSnapshot.Column) -> CGFloat {
            let expandedWidth = maxCellTextLength(for: column)
            let characterCount = max(column.width, expandedWidth, 8)
            let cappedCount = arrayInlineLimits[column.index, default: 0] > 0
                ? min(characterCount, 96)
                : min(characterCount, 40)
            return CGFloat(cappedCount) * 8.0 + 28.0
        }

        private func maxCellTextLength(for column: TableBrowserCellWindowSnapshot.Column) -> Int {
            guard arrayInlineLimits[column.index, default: 0] > 0 else {
                return 0
            }
            return grid.rows
                .compactMap { row in row.cells.first { $0.columnIndex == column.index }?.display.count }
                .max() ?? 0
        }

        func numberOfRows(in tableView: NSTableView) -> Int {
            grid.rowCount
        }

        func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
            guard let tableColumn else {
                return nil
            }
            let cellView = tableView.makeView(withIdentifier: cellIdentifier, owner: self) as? NSTableCellView
                ?? makeCellView()
            cellView.textField?.stringValue = displayValue(for: tableColumn, row: row)
            cellView.textField?.alignment = tableColumn.identifier.rawValue == "row" ? .right : .left
            cellView.textField?.font = NSFont.monospacedSystemFont(ofSize: CGFloat(interfaceFontSize), weight: .regular)
            cellView.textField?.textColor = .labelColor
            cellView.wantsLayer = true
            if tableColumn.identifier.rawValue == "row", row == localSelectedRow {
                cellView.layer?.backgroundColor = NSColor.controlAccentColor.withAlphaComponent(0.15).cgColor
            } else if let columnIndex = columnIndex(for: tableColumn),
                      row == localSelectedRow,
                      columnIndex == localSelectedColumn {
                cellView.layer?.backgroundColor = NSColor.controlAccentColor.withAlphaComponent(0.22).cgColor
            } else {
                cellView.layer?.backgroundColor = NSColor.clear.cgColor
            }
            cellView.toolTip = cellView.textField?.stringValue
            return cellView
        }

        func tableViewSelectionDidChange(_ notification: Notification) {
            // Cell clicks are handled by the explicit table action. AppKit also
            // changes row selection on click, and treating that as a second cell
            // selection makes the grid feel laggy on large tables.
        }

        @objc func tableCellClicked(_ sender: NSTableView) {
            selectClickedCell()
        }

        @objc func tableCellDoubleClicked(_ sender: NSTableView) {
            selectClickedCell()
            showCellValue(row: sender.clickedRow, tableColumnIndex: sender.clickedColumn)
        }

        private func selectClickedCell() {
            guard let tableView else {
                return
            }
            let row = tableView.clickedRow >= 0 ? tableView.clickedRow : tableView.selectedRow
            guard row >= 0 else {
                return
            }
            let clickedColumn = tableView.clickedColumn
            selectCellAt(row: row, tableColumnIndex: clickedColumn)
        }

        private func selectCellAt(row: Int, tableColumnIndex: Int) {
            guard let tableView else {
                return
            }
            let targetColumn = tableColumnIndex >= 0 && tableColumnIndex < tableView.tableColumns.count
                ? columnIndex(for: tableView.tableColumns[tableColumnIndex])
                : localSelectedColumn
            let previousRow = localSelectedRow
            let previousColumn = localSelectedColumn
            localSelectedRow = row
            localSelectedColumn = targetColumn
            reloadSelectionHighlights(previousRow: previousRow, previousColumn: previousColumn)
            selectCell(row, previousColumn, targetColumn)
        }

        private func reloadSelectionHighlights(previousRow: Int?, previousColumn: Int?) {
            guard let tableView else {
                return
            }
            var rows = IndexSet()
            for row in [previousRow, localSelectedRow].compactMap({ $0 }) where row >= 0 && row < grid.rowCount {
                rows.insert(row)
            }
            var columns = IndexSet(integer: 0)
            for column in [previousColumn, localSelectedColumn].compactMap({ $0 }) {
                if let tableIndex = tableView.tableColumns.firstIndex(where: { $0.identifier.rawValue == "column-\(column)" }) {
                    columns.insert(tableIndex)
                }
            }
            if !rows.isEmpty {
                tableView.reloadData(forRowIndexes: rows, columnIndexes: columns)
            }
        }

        func syncLocalSelectionFromState() {
            if localSelectedRow != selectedRow || localSelectedColumn != selectedColumn {
                let previousRow = localSelectedRow
                let previousColumn = localSelectedColumn
                localSelectedRow = selectedRow
                localSelectedColumn = selectedColumn
                reloadSelectionHighlights(previousRow: previousRow, previousColumn: previousColumn)
            }
        }

        private func showCellValue(row: Int, tableColumnIndex: Int) {
            guard let tableView,
                  row >= 0,
                  tableColumnIndex >= 0,
                  tableColumnIndex < tableView.tableColumns.count,
                  let columnIndex = columnIndex(for: tableView.tableColumns[tableColumnIndex]),
                  let column = grid.columns.first(where: { $0.index == columnIndex })
            else {
                return
            }
            let fallback = displayValue(for: tableView.tableColumns[tableColumnIndex], row: row)
            loadCellValue(row, columnIndex) { [inspectCell] value in
                inspectCell(TableBrowserCellInspectorItem(
                    rowIndex: row,
                    column: column,
                    value: value ?? fallback
                ))
            }
        }

        func cellMenu(for tableView: TableBrowserTableView, event: NSEvent) -> NSMenu? {
            let location = tableView.convert(event.locationInWindow, from: nil)
            let row = tableView.row(at: location)
            let tableColumnIndex = tableView.column(at: location)
            guard row >= 0,
                  tableColumnIndex >= 0,
                  tableColumnIndex < tableView.tableColumns.count,
                  let columnIndex = columnIndex(for: tableView.tableColumns[tableColumnIndex])
            else {
                return nil
            }
            selectCellAt(row: row, tableColumnIndex: tableColumnIndex)
            let menu = NSMenu(title: "Cell")
            let copy = NSMenuItem(title: "Copy Cell", action: #selector(copyContextCell(_:)), keyEquivalent: "")
            copy.target = self
            copy.representedObject = ["row": row, "column": columnIndex]
            menu.addItem(copy)
            let show = NSMenuItem(title: "Show Cell Value", action: #selector(showContextCell(_:)), keyEquivalent: "")
            show.target = self
            show.representedObject = ["row": row, "tableColumn": tableColumnIndex]
            menu.addItem(show)
            return menu
        }

        func copySelectedCell() {
            guard let row = localSelectedRow,
                  let column = localSelectedColumn
            else {
                return
            }
            copyCellValue(row, column)
        }

        @objc private func copyContextCell(_ sender: NSMenuItem) {
            guard let payload = sender.representedObject as? [String: Int],
                  let row = payload["row"],
                  let column = payload["column"]
            else {
                return
            }
            copyCellValue(row, column)
        }

        @objc private func showContextCell(_ sender: NSMenuItem) {
            guard let payload = sender.representedObject as? [String: Int],
                  let row = payload["row"],
                  let tableColumn = payload["tableColumn"]
            else {
                return
            }
            showCellValue(row: row, tableColumnIndex: tableColumn)
        }

        func headerMenu(for headerView: NSTableHeaderView, event: NSEvent) -> NSMenu? {
            let location = headerView.convert(event.locationInWindow, from: nil)
            let tableColumnIndex = headerView.column(at: location)
            guard let tableView = headerView.tableView,
                  tableColumnIndex >= 0,
                  tableColumnIndex < tableView.tableColumns.count,
                  let tableColumn = tableView.tableColumns[safe: tableColumnIndex],
                  let columnIndex = columnIndex(for: tableColumn),
                  let column = grid.columns.first(where: { $0.index == columnIndex })
            else {
                return nil
            }

            let menu = NSMenu(title: column.name)
            let title = NSMenuItem(title: column.name, action: nil, keyEquivalent: "")
            title.isEnabled = false
            menu.addItem(title)
            let summary = NSMenuItem(title: column.summary, action: nil, keyEquivalent: "")
            summary.isEnabled = false
            menu.addItem(summary)
            menu.addItem(.separator())

            let keywords = NSMenuItem(
                title: "Show Column Keywords",
                action: #selector(showHeaderColumnKeywords(_:)),
                keyEquivalent: ""
            )
            keywords.target = self
            keywords.representedObject = column.index
            keywords.isEnabled = !column.keywords.isEmpty
            menu.addItem(keywords)

            let hide = NSMenuItem(title: "Hide Column", action: #selector(hideHeaderColumn(_:)), keyEquivalent: "")
            hide.target = self
            hide.representedObject = column.index
            menu.addItem(hide)

            menu.addItem(.separator())
            addArrayExpansionItem("Array Expansion Off", limit: 0, column: column, to: menu)
            addArrayExpansionItem("Expand Arrays <= 4", limit: 4, column: column, to: menu)
            addArrayExpansionItem("Expand Arrays <= 16", limit: 16, column: column, to: menu)
            addArrayExpansionItem("Expand Arrays <= 64", limit: 64, column: column, to: menu)
            return menu
        }

        private func addArrayExpansionItem(
            _ title: String,
            limit: Int,
            column: TableBrowserCellWindowSnapshot.Column,
            to menu: NSMenu
        ) {
            let item = NSMenuItem(title: title, action: #selector(setHeaderArrayExpansion(_:)), keyEquivalent: "")
            item.target = self
            item.representedObject = ["column": column.index, "limit": limit]
            item.state = arrayInlineLimits[column.index, default: 0] == limit ? .on : .off
            menu.addItem(item)
        }

        @objc private func showHeaderColumnKeywords(_ sender: NSMenuItem) {
            guard let columnIndex = sender.representedObject as? Int,
                  let column = grid.columns.first(where: { $0.index == columnIndex })
            else {
                return
            }
            inspectColumn(column)
        }

        @objc private func hideHeaderColumn(_ sender: NSMenuItem) {
            guard let columnIndex = sender.representedObject as? Int else {
                return
            }
            setColumnHidden(columnIndex, true)
        }

        @objc private func setHeaderArrayExpansion(_ sender: NSMenuItem) {
            guard let payload = sender.representedObject as? [String: Int],
                  let columnIndex = payload["column"],
                  let limit = payload["limit"]
            else {
                return
            }
            setArrayInlineLimit(columnIndex, limit)
        }

        func restoreSelection() {
            guard let tableView, let selectedRow, selectedRow >= 0, selectedRow < grid.rowCount else {
                return
            }
            if tableView.selectedRow != selectedRow {
                tableView.selectRowIndexes(IndexSet(integer: selectedRow), byExtendingSelection: false)
            }
        }

        @objc private func boundsDidChange(_ notification: Notification) {
            requestVisibleWindowIfNeeded()
        }

        func requestVisibleWindowIfNeeded() {
            guard let tableView else {
                return
            }
            let visibleRect = tableView.visibleRect
            let visibleRows = tableView.rows(in: visibleRect)
            guard visibleRows.location != NSNotFound, visibleRows.length > 0 else {
                return
            }
            let visibleColumns = tableView.columnIndexes(in: visibleRect)
                .compactMap { tableIndex -> Int? in
                    guard tableIndex >= 0, tableIndex < tableView.tableColumns.count else {
                        return nil
                    }
                    return columnIndex(for: tableView.tableColumns[tableIndex])
                }
            let firstColumn = visibleColumns.min() ?? 0
            let lastColumn = visibleColumns.max() ?? firstColumn
            let rowStart = max(0, visibleRows.location - 256)
            let rowLimit = max(visibleRows.length + 512, 1024)
            let columnStart = max(0, firstColumn - 4)
            let columnLimit = max(lastColumn - firstColumn + 1 + 8, 24)
            requestWindowIfNeeded(
                rowStart: rowStart,
                rowLimit: rowLimit,
                columnStart: columnStart,
                columnLimit: columnLimit
            )
        }

        private func requestWindowIfNeeded(rowStart: Int, rowLimit: Int, columnStart: Int, columnLimit: Int) {
            if grid.contains(rowStart: rowStart, rowLimit: rowLimit, columnStart: columnStart, columnLimit: columnLimit) {
                lastRequestedWindow = nil
                return
            }
            let key = "\(rowStart):\(rowLimit):\(columnStart):\(columnLimit)"
            guard key != lastRequestedWindow else {
                return
            }
            lastRequestedWindow = key
            pendingRequestWorkItem?.cancel()
            let workItem = DispatchWorkItem { [weak self] in
                self?.requestCellWindow(rowStart, rowLimit, columnStart, columnLimit)
            }
            pendingRequestWorkItem = workItem
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.05, execute: workItem)
        }

        private func columnIndex(for tableColumn: NSTableColumn) -> Int? {
            let value = tableColumn.identifier.rawValue
            guard value.hasPrefix("column-") else {
                return nil
            }
            return Int(value.dropFirst("column-".count))
        }

        private func displayValue(for tableColumn: NSTableColumn, row: Int) -> String {
            if tableColumn.identifier.rawValue == "row" {
                return String(row)
            }
            guard let columnIndex = columnIndex(for: tableColumn) else {
                return ""
            }
            return cellValues["\(row):\(columnIndex)"] ?? ""
        }

        private func makeCellView() -> NSTableCellView {
            let cellView = NSTableCellView()
            cellView.identifier = cellIdentifier
            let textField = NSTextField(labelWithString: "")
            textField.translatesAutoresizingMaskIntoConstraints = false
            textField.font = NSFont.monospacedSystemFont(ofSize: NSFont.smallSystemFontSize, weight: .regular)
            textField.lineBreakMode = .byTruncatingTail
            textField.maximumNumberOfLines = 1
            textField.allowsDefaultTighteningForTruncation = false
            cellView.addSubview(textField)
            cellView.textField = textField
            NSLayoutConstraint.activate([
                textField.leadingAnchor.constraint(equalTo: cellView.leadingAnchor, constant: 6),
                textField.trailingAnchor.constraint(equalTo: cellView.trailingAnchor, constant: -6),
                textField.centerYAnchor.constraint(equalTo: cellView.centerYAnchor)
            ])
            return cellView
        }
    }
}

private struct TableBrowserCellsGrid: View {
    let table: TableBrowserRenderedCellTable
    let selectCell: (_ rowIndex: Int?, _ selectedVisibleColumn: Int?, _ targetVisibleColumn: Int?) -> Void

    var body: some View {
        ScrollView([.horizontal, .vertical]) {
            VStack(alignment: .leading, spacing: 0) {
                HStack(spacing: 0) {
                    ForEach(Array(table.headers.enumerated()), id: \.offset) { index, header in
                        gridText(header.isEmpty ? " " : header, header: true)
                            .frame(width: index == 0 ? 56 : columnWidth(for: header), alignment: .leading)
                    }
                }
                ForEach(table.rows) { row in
                    HStack(spacing: 0) {
                        gridText(row.rowIndex.map(String.init) ?? "", selected: row.selectedRow)
                            .frame(width: 56, alignment: .trailing)
                            .contentShape(Rectangle())
                            .onTapGesture {
                                selectCell(row.rowIndex, selectedVisibleColumn, selectedVisibleColumn)
                            }
                        ForEach(Array(row.cells.enumerated()), id: \.offset) { index, cell in
                            gridText(cell.text.isEmpty ? " " : cell.text, selected: cell.selected || row.selectedRow)
                                .frame(width: columnWidth(for: table.headers[safe: index + 1] ?? cell.text), alignment: .leading)
                                .contentShape(Rectangle())
                                .onTapGesture {
                                    selectCell(row.rowIndex, selectedVisibleColumn, index)
                                }
                        }
                    }
                }
            }
            .frame(maxWidth: .infinity, alignment: .topLeading)
        }
    }

    private var selectedVisibleColumn: Int? {
        table.rows
            .compactMap { row in row.cells.firstIndex(where: \.selected) }
            .first
    }

    private func columnWidth(for text: String) -> CGFloat {
        CGFloat(min(max(text.count, 10), 30)) * 8.0 + 18.0
    }

    private func gridText(_ text: String, header: Bool = false, selected: Bool = false) -> some View {
        Text(text)
            .workbenchFont(.caption, weight: header ? .semibold : .regular, design: .monospaced)
            .lineLimit(1)
            .padding(.horizontal, 6)
            .padding(.vertical, 3)
            .background(selected ? Color.accentColor.opacity(0.18) : (header ? Color.secondary.opacity(0.10) : Color.clear))
            .overlay(Rectangle().stroke(Color.secondary.opacity(0.18), lineWidth: 0.5))
    }
}

private struct TableBrowserKeyValueGrid: View {
    let lines: [String]
    let selectedIndex: Int?
    let selectMainItem: (Int) -> Void

    var body: some View {
        ScrollView([.horizontal, .vertical]) {
            VStack(alignment: .leading, spacing: 0) {
                headerRow(["Owner", "Keyword", "Value"])
                ForEach(keywordRows) { row in
                    HStack(spacing: 0) {
                        gridText(row.owner, selected: row.selected).frame(width: 150, alignment: .leading)
                        gridText(row.name, selected: row.selected).frame(width: 240, alignment: .leading)
                        gridText(row.value, selected: row.selected).frame(width: 420, alignment: .leading)
                    }
                    .contentShape(Rectangle())
                    .onTapGesture {
                        selectMainItem(row.index)
                    }
                }
            }
            .frame(maxWidth: .infinity, alignment: .topLeading)
        }
    }

    private var keywordRows: [KeywordRow] {
        let visibleRows = lines.compactMap(KeywordRow.init(line:))
        return Self.assignAbsoluteIndexes(visibleRows, selectedIndex: selectedIndex)
    }

    private static func assignAbsoluteIndexes(_ rows: [KeywordRow], selectedIndex: Int?) -> [KeywordRow] {
        guard let selectedIndex,
              let selectedVisibleOffset = rows.firstIndex(where: \.selected)
        else {
            return rows.enumerated().map { offset, row in
                var row = row
                row.index = offset
                return row
            }
        }
        let firstIndex = selectedIndex - selectedVisibleOffset
        return rows.enumerated().map { offset, row in
            var row = row
            row.index = max(0, firstIndex + offset)
            return row
        }
    }

    private func headerRow(_ labels: [String]) -> some View {
        HStack(spacing: 0) {
            gridText(labels[0], header: true).frame(width: 150, alignment: .leading)
            gridText(labels[1], header: true).frame(width: 240, alignment: .leading)
            gridText(labels[2], header: true).frame(width: 420, alignment: .leading)
        }
    }

    private func gridText(_ text: String, header: Bool = false, selected: Bool = false) -> some View {
        Text(text.isEmpty ? " " : text)
            .workbenchFont(.caption, weight: header ? .semibold : .regular, design: .monospaced)
            .lineLimit(1)
            .padding(.horizontal, 6)
            .padding(.vertical, 3)
            .background(selected ? Color.accentColor.opacity(0.18) : (header ? Color.secondary.opacity(0.10) : Color.clear))
            .overlay(Rectangle().stroke(Color.secondary.opacity(0.18), lineWidth: 0.5))
    }

    private struct KeywordRow: Identifiable {
        var id: Int { index }
        var index: Int
        var selected: Bool
        var owner: String
        var name: String
        var value: String

        init?(line: String) {
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            guard !trimmed.isEmpty, !trimmed.hasPrefix("Keywords "), !trimmed.hasPrefix("--") else {
                return nil
            }
            selected = trimmed.hasPrefix(">")
            let markerStripped = trimmed.dropFirst(selected ? 1 : 0).trimmingCharacters(in: .whitespaces)
            let parts = markerStripped.split(separator: "=", maxSplits: 1, omittingEmptySubsequences: false)
            guard let lhs = parts.first else {
                return nil
            }
            value = parts.dropFirst().first.map { String($0).trimmingCharacters(in: .whitespaces) } ?? ""
            let ownerParts = lhs.split(separator: ".", maxSplits: 1, omittingEmptySubsequences: false)
            owner = ownerParts.first.map(String.init) ?? ""
            name = ownerParts.dropFirst().first.map { String($0).trimmingCharacters(in: .whitespaces) } ?? ""
            index = 0
        }
    }
}

private struct TableBrowserSubtableGrid: View {
    let lines: [String]
    let selectMainItem: (Int) -> Void
    let openSelectedSubtable: () -> Void

    var body: some View {
        ScrollView([.horizontal, .vertical]) {
            VStack(alignment: .leading, spacing: 0) {
                headerRow
                ForEach(subtableRows) { row in
                    HStack(spacing: 0) {
                        gridText(String(row.index), selected: row.selected).frame(width: 64, alignment: .trailing)
                        gridText(row.label, selected: row.selected).frame(width: 260, alignment: .leading)
                        gridText(row.source, selected: row.selected).frame(width: 420, alignment: .leading)
                    }
                    .contentShape(Rectangle())
                    .onTapGesture {
                        selectMainItem(row.index)
                    }
                    .onTapGesture(count: 2) {
                        selectMainItem(row.index)
                        openSelectedSubtable()
                    }
                }
            }
            .frame(maxWidth: .infinity, alignment: .topLeading)
        }
    }

    private var headerRow: some View {
        HStack(spacing: 0) {
            gridText("#", header: true).frame(width: 64, alignment: .trailing)
            gridText("Subtable", header: true).frame(width: 260, alignment: .leading)
            gridText("Source", header: true).frame(width: 420, alignment: .leading)
        }
    }

    private var subtableRows: [SubtableRow] {
        lines.compactMap(SubtableRow.init(line:))
    }

    private func gridText(_ text: String, header: Bool = false, selected: Bool = false) -> some View {
        Text(text.isEmpty ? " " : text)
            .workbenchFont(.caption, weight: header ? .semibold : .regular, design: .monospaced)
            .lineLimit(1)
            .padding(.horizontal, 6)
            .padding(.vertical, 3)
            .background(selected ? Color.accentColor.opacity(0.18) : (header ? Color.secondary.opacity(0.10) : Color.clear))
            .overlay(Rectangle().stroke(Color.secondary.opacity(0.18), lineWidth: 0.5))
    }

    private struct SubtableRow: Identifiable {
        var id: Int { index }
        var index: Int
        var selected: Bool
        var label: String
        var source: String

        init?(line: String) {
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            guard !trimmed.isEmpty, !trimmed.hasPrefix("Subtables "), !trimmed.hasPrefix("--") else {
                return nil
            }
            guard let openBracket = trimmed.firstIndex(of: "["),
                  let closeBracket = trimmed.firstIndex(of: "]")
            else {
                return nil
            }
            selected = trimmed.hasPrefix(">")
            let indexText = trimmed[trimmed.index(after: openBracket)..<closeBracket]
            guard let parsedIndex = Int(indexText) else {
                return nil
            }
            index = parsedIndex
            let rest = trimmed[trimmed.index(after: closeBracket)...].trimmingCharacters(in: .whitespaces)
            if let sourceStart = rest.lastIndex(of: "("), rest.hasSuffix(")") {
                label = String(rest[..<sourceStart]).trimmingCharacters(in: .whitespaces)
                source = String(rest[rest.index(after: sourceStart)..<rest.index(before: rest.endIndex)])
            } else {
                label = rest
                source = ""
            }
        }
    }
}

private struct TableBrowserInspectorPane: View {
    let inspector: TableBrowserSnapshot.Inspector?

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(inspector?.title ?? "Inspector")
                .workbenchFont(.headline)
            if let inspector {
                if !inspector.trail.isEmpty {
                    Text(inspector.trail.map(\.label).joined(separator: " / "))
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                }
                TableBrowserTypedInspector(node: inspector.node)
                Divider()
                ScrollView {
                    VStack(alignment: .leading, spacing: 3) {
                        ForEach(Array(inspector.renderedLines.enumerated()), id: \.offset) { _, line in
                            Text(line.isEmpty ? " " : line)
                                .workbenchFont(.caption, design: .monospaced)
                                .lineLimit(1)
                                .frame(maxWidth: .infinity, alignment: .leading)
                        }
                    }
                }
            } else {
                Text("No selected value")
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
            }
        }
        .padding(12)
        .background(Color(nsColor: .windowBackgroundColor))
        .clipShape(RoundedRectangle(cornerRadius: 6))
        .overlay(RoundedRectangle(cornerRadius: 6).stroke(Color.secondary.opacity(0.18)))
    }
}

private struct TableBrowserTypedInspector: View {
    let node: TableBrowserSnapshot.ValueNode

    var body: some View {
        switch node {
        case .undefined:
            typedLine("Type", "undefined")
        case let .scalar(value):
            typedLine("Scalar", value.displayString)
        case let .array(primitive, shape, totalElements, pageStart, pageSize, elements):
            VStack(alignment: .leading, spacing: 4) {
                typedLine("Array", "\(primitive) \(shape.map(String.init).joined(separator: " x "))")
                typedLine("Elements", "\(pageStart + 1)-\(min(pageStart + pageSize, totalElements)) of \(totalElements)")
                ForEach(Array(elements.prefix(4).enumerated()), id: \.offset) { _, element in
                    Text("\(element.selected ? ">" : " ") [\(element.index.map(String.init).joined(separator: ","))] \(element.value.displayString)")
                        .workbenchFont(.caption, design: .monospaced)
                        .lineLimit(1)
                }
            }
        case let .record(totalFields, pageStart, pageSize, fields):
            VStack(alignment: .leading, spacing: 4) {
                typedLine("Record", "\(pageStart + 1)-\(min(pageStart + pageSize, totalFields)) of \(totalFields) fields")
                ForEach(Array(fields.prefix(5).enumerated()), id: \.offset) { _, field in
                    Text("\(field.selected ? ">" : " ") \(field.name): \(field.summary)")
                        .workbenchFont(.caption, design: .monospaced)
                        .lineLimit(1)
                }
            }
        case let .tableRef(path, resolvedPath, openable):
            VStack(alignment: .leading, spacing: 4) {
                typedLine("Table ref", path)
                typedLine(openable ? "Openable" : "Unavailable", resolvedPath)
            }
        }
    }

    private func typedLine(_ label: String, _ value: String) -> some View {
        HStack(alignment: .firstTextBaseline, spacing: 8) {
            Text(label)
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
                .frame(width: 72, alignment: .leading)
            Text(value)
                .workbenchFont(.caption, design: .monospaced)
                .lineLimit(1)
        }
    }
}

private extension TableBrowserSnapshot.ScalarValue {
    var displayString: String {
        switch self {
        case let .bool(value):
            return value ? "true" : "false"
        case let .int(value):
            return String(value)
        case let .uint(value):
            return String(value)
        case let .float(value):
            return String(format: "%.6g", value)
        case let .complex(re, im):
            return String(format: "%.6g%+.6gi", re, im)
        case let .string(value):
            return value
        case let .unknown(type, display):
            return display.isEmpty ? type : "\(type) \(display)"
        }
    }
}

private struct SelectionHelperOption: Identifiable, Equatable {
    var label: String
    var value: String

    var id: String { "\(label)=\(value)" }
}

enum MeasurementSetExplorerMode: String, CaseIterable, Identifiable {
    case summary
    case plots

    var id: String { rawValue }

    var title: String {
        switch self {
        case .summary: "Summary"
        case .plots: "Plots"
        }
    }
}

struct MeasurementSetPlotPanel: View {
    @ObservedObject var store: WorkbenchStore
    let dataset: DatasetSummary
    @Environment(\.workbenchFontSize) private var workbenchFontSize
    @State private var explorerMode: MeasurementSetExplorerMode = .summary
    @State private var summaryStatus: MeasurementSetPlotStatus = .idle
    @State private var summaryResult: MeasurementSetSummaryResultSummary?
    @State private var summaryError: String?
    @State private var summaryFormat = "text"
    @State private var showingAdvancedSetup = false
    @State private var showingPlotControls = false
    @State private var plotDisplayMode: WorkbenchPlotDisplayMode = .automatic
    @State private var plotCharacterSizeOverride: Double?
    @State private var maxPlotPointsText = ""
    @State private var avgChannelText = ""
    @State private var avgTimeText = ""
    @State private var activeSelectionHelper: String?
    @State private var uvRangeMinText = ""
    @State private var uvRangeMaxText = ""
    @State private var uvRangeUnit = "m"
    @State private var uvRangeScanStatus: String?
    @State private var isScanningUVRange = false
    @State private var channelStartText = ""
    @State private var channelEndText = ""
    @State private var channelStepText = ""
    @State private var timerangeStartText = ""
    @State private var timerangeEndText = ""
    @State private var timerangeScanStatus: String?
    @State private var isScanningTimerange = false
    @State private var integerRangeStartText = ""
    @State private var integerRangeEndText = ""
    @State private var msSelectColumn = ""
    @State private var msSelectOperator = "=="
    @State private var msSelectValue = ""
    @State private var avgTimeUnit = "s"
    private let metadataClient: MeasurementSetMetadataClient = UniFFIMeasurementSetMetadataClient()
    private let summaryClient: MeasurementSetSummaryClient = UniFFIMeasurementSetSummaryClient()

    init(
        store: WorkbenchStore,
        dataset: DatasetSummary,
        initialExplorerMode: MeasurementSetExplorerMode = .summary
    ) {
        self.store = store
        self.dataset = dataset
        _explorerMode = State(initialValue: initialExplorerMode)
    }

    var body: some View {
        ZStack(alignment: .top) {
            if explorerMode == .summary {
                summarySurface
            } else {
                plotSurface
            }
            explorerCommandBar
                .padding(.top, 14)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .accessibilityIdentifier("msPlot.panel.\(dataset.id)")
        .onAppear {
            if summaryStatus == .idle {
                runMeasurementSetSummary()
            }
        }
        .onChange(of: explorerMode) { mode in
            if mode == .summary && summaryResult == nil && summaryStatus != .running {
                runMeasurementSetSummary()
            }
        }
        .onChange(of: summaryFormat) { _ in
            if explorerMode == .summary {
                runMeasurementSetSummary()
            }
        }
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

    private var explorerCommandBar: some View {
        HStack(spacing: 10) {
            Picker("View", selection: $explorerMode) {
                ForEach(MeasurementSetExplorerMode.allCases) { mode in
                    Text(mode.title).tag(mode)
                }
            }
            .labelsHidden()
            .pickerStyle(.segmented)
            .frame(width: 170)
            .accessibilityIdentifier("msExplore.mode.\(dataset.id)")

            if explorerMode == .plots {
                Picker("Plot", selection: Binding(
                    get: { plotState.preset },
                    set: { store.setMeasurementSetPlotPreset($0, datasetID: dataset.id) }
                )) {
                    ForEach(MeasurementSetExplorerPlotPreset.menuCases) { preset in
                        Text(preset.title).tag(preset)
                    }
                }
                .labelsHidden()
                .frame(width: 220)
                .accessibilityIdentifier("msPlot.preset.\(dataset.id)")
            } else {
                Picker("Format", selection: $summaryFormat) {
                    Text("Text").tag("text")
                    Text("JSON").tag("json")
                }
                .labelsHidden()
                .frame(width: 105)
                .accessibilityIdentifier("msSummary.format.\(dataset.id)")
            }

            Button {
                showingAdvancedSetup.toggle()
            } label: {
                Label("Selections", systemImage: "slider.horizontal.3")
            }
            .popover(isPresented: $showingAdvancedSetup, arrowEdge: .top) {
                ScrollView {
                    plotSelections
                }
                .frame(width: 360, height: 680)
            }
            .accessibilityIdentifier("msPlot.selections.\(dataset.id)")

            if explorerMode == .summary {
                Button {
                    runMeasurementSetSummary()
                } label: {
                    Label(summaryStatus == .running ? "Refreshing" : "Refresh", systemImage: "arrow.clockwise")
                }
                .disabled(summaryStatus == .running)
                .accessibilityIdentifier("msSummary.refresh.\(dataset.id)")

                Button {
                    saveMeasurementSetSummary()
                } label: {
                    Label("Save", systemImage: "square.and.arrow.down")
                }
                .disabled(summaryResult == nil || summaryResult?.format != summaryFormat)
                .accessibilityIdentifier("msSummary.save.\(dataset.id)")
            } else {
                Button {
                    store.runMeasurementSetPlot(datasetID: dataset.id)
                } label: {
                    Label(plotState.status == .running ? "Generating" : "Generate", systemImage: "play.fill")
                }
                .disabled(plotState.status == .running)
                .accessibilityIdentifier("msPlot.generate.\(dataset.id)")
            }
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

            selectionTextField(
                "Channels",
                text: Binding(
                    get: { plotState.selectedChannelSelection ?? "" },
                    set: { store.setMeasurementSetPlotChannelSelection($0, datasetID: dataset.id) }
                ),
                prompt: "3~7;12 or 0~63^4",
                systemImage: "number",
                help: "CASA channel syntax: channel, start~end, or start~end^step. Values are limited to the selected spectral window.",
                helperOptions: channelSelectionOptions,
                validator: isValidChannelSelection
            )

            selectionTextField(
                "Timerange",
                text: Binding(
                    get: { plotState.selectedTimerange ?? "" },
                    set: { store.setMeasurementSetPlotTimerange($0, datasetID: dataset.id) }
                ),
                prompt: "CASA timerange",
                systemImage: "clock",
                help: "CASA timerange syntax. This helper builds numeric MJD-second ranges from MAIN.TIME, for example 4860027194~4860033280.",
                helperOptions: [SelectionHelperOption(label: "All times", value: "")],
                validator: isValidTimerangeSelection
            )

            selectionTextField(
                "UV range",
                text: Binding(
                    get: { plotState.selectedUVRange ?? "" },
                    set: { store.setMeasurementSetPlotUVRange($0, datasetID: dataset.id) }
                ),
                prompt: "CASA uvrange",
                systemImage: "ruler",
                help: "CASA UV range syntax: min~maxm, >100m, <2klambda, or 0~1klambda. Supported units are m and klambda here.",
                helperOptions: [SelectionHelperOption(label: "All UV distances", value: "")],
                validator: isValidUVRangeSelection
            )

            selectionTextField(
                "Antenna",
                text: Binding(
                    get: { plotState.selectedAntenna ?? "" },
                    set: { store.setMeasurementSetPlotAntenna($0, datasetID: dataset.id) }
                ),
                prompt: "CASA antenna",
                systemImage: "antenna.radiowaves.left.and.right",
                help: "CASA antenna syntax: antenna name, numeric antenna id, comma list, or baseline with &. Values must exist in this MS.",
                helperOptions: antennaSelectionOptions,
                validator: isValidAntennaSelection
            )

            selectionTextField(
                "Scan",
                text: Binding(
                    get: { plotState.selectedScan ?? "" },
                    set: { store.setMeasurementSetPlotScan($0, datasetID: dataset.id) }
                ),
                prompt: "CASA scan",
                systemImage: "rectangle.stack",
                help: "CASA scan syntax: scan number, comma list, or numeric range. Values must exist in this MS.",
                helperOptions: scanSelectionOptions,
                validator: { isValidIntegerSelection($0, labels: dataset.scans) }
            )

            selectionTextField(
                "Array",
                text: Binding(
                    get: { plotState.selectedArray ?? "" },
                    set: { store.setMeasurementSetPlotArray($0, datasetID: dataset.id) }
                ),
                prompt: "CASA array",
                systemImage: "square.grid.3x3",
                help: "CASA array syntax: array id, comma list, or numeric range. Values must exist in this MS.",
                helperOptions: arraySelectionOptions,
                validator: { isValidIntegerSelection($0, labels: dataset.arrays) }
            )

            selectionTextField(
                "Observation",
                text: Binding(
                    get: { plotState.selectedObservation ?? "" },
                    set: { store.setMeasurementSetPlotObservation($0, datasetID: dataset.id) }
                ),
                prompt: "CASA observation",
                systemImage: "eye",
                help: "CASA observation syntax: observation id, comma list, or numeric range. Values must exist in this MS.",
                helperOptions: observationSelectionOptions,
                validator: { isValidIntegerSelection($0, labels: dataset.observations) }
            )

            selectionTextField(
                "Intent",
                text: Binding(
                    get: { plotState.selectedIntent ?? "" },
                    set: { store.setMeasurementSetPlotIntent($0, datasetID: dataset.id) }
                ),
                prompt: "CASA intent",
                systemImage: "tag",
                help: "CASA intent selection. Choose an OBS_MODE value present in this MS, or leave empty for all intents.",
                helperOptions: intentSelectionOptions,
                validator: isValidIntentSelection
            )

            selectionTextField(
                "Feed",
                text: Binding(
                    get: { plotState.selectedFeed ?? "" },
                    set: { store.setMeasurementSetPlotFeed($0, datasetID: dataset.id) }
                ),
                prompt: "CASA feed",
                systemImage: "dot.radiowaves.left.and.right",
                help: "CASA feed syntax: feed id, comma list, or numeric range. Values must exist in this MS.",
                helperOptions: feedSelectionOptions,
                validator: { isValidIntegerSelection($0, labels: dataset.feeds) }
            )

            selectionTextField(
                "MS select",
                text: Binding(
                    get: { plotState.selectedMSSelect ?? "" },
                    set: { store.setMeasurementSetPlotMSSelect($0, datasetID: dataset.id) }
                ),
                prompt: "TaQL/MSSelection",
                systemImage: "curlybraces",
                help: "Advanced TaQL/MSSelection expression. Leave empty unless you need a selector not covered by the guided fields.",
                helperOptions: [SelectionHelperOption(label: "No advanced selector", value: "")]
            )

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

            Picker("Color By", selection: Binding(
                get: { plotState.colorBy },
                set: { store.setMeasurementSetPlotColorBy($0, datasetID: dataset.id) }
            )) {
                ForEach(MeasurementSetPlotColorAxis.allCases) { axis in
                    Text(axis.title).tag(axis)
                }
            }
            .help("CASA plotms color/group axis. Field groups source fields; Baseline groups antenna pairs; None draws one series.")
            .accessibilityIdentifier("msPlot.colorBy.\(dataset.id)")

            Picker("Iterate by", selection: Binding<MeasurementSetPlotIterationAxis?>(
                get: { plotState.iterationAxis },
                set: { store.setMeasurementSetPlotIterationAxis($0, datasetID: dataset.id) }
            )) {
                Text("None").tag(nil as MeasurementSetPlotIterationAxis?)
                ForEach(MeasurementSetPlotIterationAxis.allCases) { axis in
                    Text(axis.title).tag(Optional(axis))
                }
            }
            .help("CASA plotms Page tab Iteration Axis. Field splits amplitude/UV-distance plots into one panel per source field.")
            .accessibilityIdentifier("msPlot.iterationAxis.\(dataset.id)")

            VStack(alignment: .leading, spacing: 6) {
                HStack {
                    Text("Max plotted points")
                    Spacer()
                    TextField("250k", text: $maxPlotPointsText)
                        .multilineTextAlignment(.trailing)
                        .textFieldStyle(.roundedBorder)
                        .foregroundColor(maxPlotPointsTextIsWarning ? .yellow : .primary)
                        .frame(width: 96)
                        .onSubmit {
                            applyMaxPlotPointsText()
                        }
                }
                .accessibilityIdentifier("msPlot.maxPlotPoints.\(dataset.id)")
                Text("Accepts plain counts, k, or M. Values above 5M are highlighted as expensive.")
                    .workbenchFont(.caption)
                    .foregroundStyle(maxPlotPointsTextIsWarning ? .yellow : .secondary)
            }

            Divider()

            VStack(alignment: .leading, spacing: 8) {
                Text("Averaging")
                    .workbenchFont(.subheadline, weight: .semibold)

                selectionTextField(
                    "Avg channel",
                    text: $avgChannelText,
                    prompt: "bin size",
                    systemImage: "number.square",
                    help: "Positive channel averaging bin. Values are limited to the selected spectral window channel count when known.",
                    helperOptions: avgChannelOptions,
                    validator: isValidAvgChannelText
                )
                .onSubmit {
                    applyAveragingText()
                }

                selectionTextField(
                    "Avg time",
                    text: $avgTimeText,
                    prompt: "seconds",
                    systemImage: "timer",
                    help: "Positive averaging interval in seconds.",
                    helperOptions: avgTimeOptions,
                    validator: isValidPositiveSecondsText
                )
                .onSubmit {
                    applyAveragingText()
                }

                Toggle("Average across scans", isOn: Binding(
                    get: { plotState.avgScan },
                    set: { store.setMeasurementSetPlotAvgScan($0, datasetID: dataset.id) }
                ))
                Toggle("Average across fields", isOn: Binding(
                    get: { plotState.avgField },
                    set: { store.setMeasurementSetPlotAvgField($0, datasetID: dataset.id) }
                ))
                Toggle("Average across baselines", isOn: Binding(
                    get: { plotState.avgBaseline },
                    set: { store.setMeasurementSetPlotAvgBaseline($0, datasetID: dataset.id) }
                ))
                Toggle("Average across antennas", isOn: Binding(
                    get: { plotState.avgAntenna },
                    set: { store.setMeasurementSetPlotAvgAntenna($0, datasetID: dataset.id) }
                ))
                Toggle("Average across spectral windows", isOn: Binding(
                    get: { plotState.avgSPW },
                    set: { store.setMeasurementSetPlotAvgSPW($0, datasetID: dataset.id) }
                ))
                Toggle("Scalar average", isOn: Binding(
                    get: { plotState.scalarAverage },
                    set: { store.setMeasurementSetPlotScalarAverage($0, datasetID: dataset.id) }
                ))
            }

            Divider()

            plotMetadata
        }
        .padding(16)
        .onAppear {
            maxPlotPointsText = Self.formatPointBudget(plotState.maxPlotPoints)
            avgChannelText = plotState.avgChannel.map { String($0) } ?? ""
            avgTimeText = plotState.avgTime.map { String($0) } ?? ""
        }
        .onDisappear {
            applyMaxPlotPointsText()
            applyAveragingText()
        }
    }

    private var plotSurface: some View {
        plotDocumentSurface
            .frame(maxWidth: .infinity, maxHeight: .infinity)
    }

    private var summarySurface: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                VStack(alignment: .leading, spacing: 2) {
                    Text("ListObs Summary")
                        .workbenchFont(.headline)
                    Text(summaryResult?.selectionSummary ?? "CASA-style MeasurementSet metadata")
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                if summaryStatus == .running {
                    ProgressView()
                        .scaleEffect(0.75)
                }
            }

            if let summaryError {
                Text(summaryError)
                    .foregroundStyle(.red)
                    .frame(maxWidth: .infinity, alignment: .leading)
            } else if let summaryResult {
                ScrollView([.vertical, .horizontal]) {
                    Text(summaryResult.summaryText)
                        .workbenchFont(.caption, design: .monospaced)
                        .textSelection(.enabled)
                        .frame(maxWidth: .infinity, alignment: .topLeading)
                        .padding(12)
                }
                .background(Color(nsColor: .textBackgroundColor))
                .overlay(RoundedRectangle(cornerRadius: 6).stroke(Color.secondary.opacity(0.20)))
                .clipShape(RoundedRectangle(cornerRadius: 6))

                if let diagnostic = summaryResult.diagnostics.first {
                    Text(diagnostic)
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                }
            } else {
                ZStack {
                    RoundedRectangle(cornerRadius: 6)
                        .fill(Color(nsColor: .windowBackgroundColor))
                        .overlay(RoundedRectangle(cornerRadius: 6).stroke(Color.secondary.opacity(0.20)))
                    VStack(spacing: 10) {
                        Image(systemName: "doc.text.magnifyingglass")
                            .workbenchFont(.largeTitle)
                        Text(summaryStatus == .running ? "Reading MeasurementSet summary" : "No summary loaded")
                            .workbenchFont(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            }
        }
        .padding(.top, 76)
        .padding(.horizontal, 16)
        .padding(.bottom, 16)
        .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        .accessibilityIdentifier("msSummary.document.\(dataset.id)")
    }

    private func runMeasurementSetSummary() {
        let request = MeasurementSetSummaryBuildRequest(
            datasetPath: dataset.path,
            format: summaryFormat,
            field: selectorToken(plotState.selectedField),
            spectralWindow: spectralWindowSelectorToken(plotState),
            timerange: plotState.selectedTimerange,
            uvRange: plotState.selectedUVRange,
            antenna: plotState.selectedAntenna,
            scan: plotState.selectedScan,
            correlation: selectorToken(plotState.selectedCorrelation),
            array: plotState.selectedArray,
            observation: plotState.selectedObservation,
            intent: plotState.selectedIntent,
            feed: plotState.selectedFeed,
            msselect: plotState.selectedMSSelect
        )
        summaryStatus = .running
        summaryError = nil
        summaryResult = nil
        Task {
            do {
                let result = try summaryClient.buildSummary(request: request)
                await MainActor.run {
                    summaryResult = result
                    summaryStatus = .ready
                    summaryError = nil
                }
            } catch {
                await MainActor.run {
                    summaryStatus = .failed
                    summaryError = "\(error)"
                }
            }
        }
    }

    private func saveMeasurementSetSummary() {
        guard let summaryResult else { return }
        let panel = NSSavePanel()
        panel.title = "Save MeasurementSet Summary"
        panel.canCreateDirectories = true
        panel.isExtensionHidden = false
        panel.directoryURL = URL(fileURLWithPath: dataset.path).deletingLastPathComponent()
        panel.nameFieldStringValue = "\(dataset.name)-listobs.\(summaryFileExtension)"
        panel.allowedContentTypes = summaryFormat == "json" ? [.json] : [.plainText]
        panel.begin { response in
            guard response == .OK, let url = panel.url else { return }
            do {
                try summaryResult.summaryText.write(to: url, atomically: true, encoding: .utf8)
            } catch {
                summaryError = "Save failed: \(error.localizedDescription)"
            }
        }
    }

    private var summaryFileExtension: String {
        summaryFormat == "json" ? "json" : "txt"
    }

    private func selectionTextField(
        _ label: String,
        text: Binding<String>,
        prompt: String,
        systemImage: String,
        help: String,
        helperOptions: [SelectionHelperOption],
        validator: @escaping (String) -> Bool = { _ in true }
    ) -> some View {
        let validatedText = Binding<String>(
            get: { text.wrappedValue },
            set: { newValue in
                if validator(newValue) {
                    text.wrappedValue = newValue
                }
            }
        )
        let isValid = validator(text.wrappedValue)
        return HStack {
            Text(label)
            Spacer()
            TextField(prompt, text: validatedText)
                .multilineTextAlignment(.trailing)
                .textFieldStyle(.roundedBorder)
                .foregroundStyle(isValid ? Color.primary : Color.red)
                .frame(width: 150)
                .help(help)
            Button {
                activeSelectionHelper = label
            } label: {
                Image(systemName: systemImage)
                    .frame(width: 18)
            }
            .buttonStyle(.borderless)
            .fixedSize()
            .help("Open guided choices for \(label.lowercased()). \(help)")
            .popover(isPresented: Binding(
                get: { activeSelectionHelper == label },
                set: { isPresented in
                    if !isPresented && activeSelectionHelper == label {
                        activeSelectionHelper = nil
                    }
                }
            )) {
                selectionHelperPopover(
                    label: label,
                    text: text,
                    help: help,
                    helperOptions: helperOptions
                )
            }
        }
    }

    @ViewBuilder
    private func selectionHelperPopover(
        label: String,
        text: Binding<String>,
        help: String,
        helperOptions: [SelectionHelperOption]
    ) -> some View {
        if label == "Channels" {
            channelHelperPopover(text: text, help: help)
        } else if label == "Timerange" {
            timerangeHelperPopover(text: text, help: help)
        } else if label == "UV range" {
            uvRangeHelperPopover(text: text, help: help)
        } else if label == "Antenna" {
            antennaHelperPopover(text: text, help: help)
        } else if ["Scan", "Array", "Observation", "Feed"].contains(label) {
            integerSetHelperPopover(label: label, text: text, help: help, helperOptions: helperOptions)
        } else if label == "Intent" {
            toggleListHelperPopover(emptyLabel: "all intents", text: text, help: help, helperOptions: helperOptions)
        } else if label == "MS select" {
            msSelectHelperPopover(text: text, help: help)
        } else if label == "Avg channel" {
            avgChannelHelperPopover(text: text, help: help)
        } else if label == "Avg time" {
            avgTimeHelperPopover(text: text, help: help)
        } else {
            defaultSelectionHelperPopover(text: text, help: help, helperOptions: helperOptions)
        }
    }

    private func defaultSelectionHelperPopover(
        text: Binding<String>,
        help: String,
        helperOptions: [SelectionHelperOption]
    ) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(help)
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
            Divider()
            Button("Clear") {
                text.wrappedValue = ""
            }
            ScrollView {
                VStack(alignment: .leading, spacing: 6) {
                    ForEach(helperOptions) { option in
                        Button(option.label) {
                            text.wrappedValue = option.value
                        }
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
            }
            .frame(maxHeight: 220)
        }
        .padding(12)
        .frame(width: 300, alignment: .leading)
    }

    private func channelHelperPopover(text: Binding<String>, help: String) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(help)
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
            if let limit = selectedSpectralWindowChannelLimit {
                Text("Valid channel IDs: 0 through \(max(0, limit - 1))")
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
            }
            HStack {
                TextField("start", text: $channelStartText)
                    .textFieldStyle(.roundedBorder)
                Text("to")
                    .foregroundStyle(.secondary)
                TextField("end", text: $channelEndText)
                    .textFieldStyle(.roundedBorder)
                Text("step")
                    .foregroundStyle(.secondary)
                TextField("1", text: $channelStepText)
                    .textFieldStyle(.roundedBorder)
                    .frame(width: 48)
            }
            HStack {
                Button("Apply range") {
                    applyChannelSelection(text: text)
                }
                Button("Clear") {
                    channelStartText = ""
                    channelEndText = ""
                    channelStepText = ""
                    text.wrappedValue = ""
                }
                Spacer()
                Button("All") {
                    text.wrappedValue = ""
                }
            }
            Divider()
            ScrollView {
                VStack(alignment: .leading, spacing: 6) {
                    ForEach(channelSelectionOptions) { option in
                        Button(option.label) {
                            text.wrappedValue = option.value
                            populateChannelFields(from: option.value)
                        }
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
            }
            .frame(maxHeight: 150)
        }
        .padding(12)
        .frame(width: 420, alignment: .leading)
        .onAppear {
            populateChannelFields(from: text.wrappedValue)
        }
    }

    private func timerangeHelperPopover(text: Binding<String>, help: String) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(help)
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
            Text("Use MJD seconds here; the generated selector is min~max. Leave one side blank for open-ended ranges.")
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
            HStack {
                TextField("start seconds", text: $timerangeStartText)
                    .textFieldStyle(.roundedBorder)
                Text("to")
                    .foregroundStyle(.secondary)
                TextField("end seconds", text: $timerangeEndText)
                    .textFieldStyle(.roundedBorder)
            }
            HStack {
                Button("Apply") {
                    applyTimerangeSelection(text: text)
                }
                Button("Clear") {
                    timerangeStartText = ""
                    timerangeEndText = ""
                    text.wrappedValue = ""
                }
                Spacer()
                Button(isScanningTimerange ? "Scanning..." : "Scan MS") {
                    scanTimerange(text: text)
                }
                .disabled(isScanningTimerange)
            }
            if let timerangeScanStatus {
                Text(timerangeScanStatus)
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
        .padding(12)
        .frame(width: 420, alignment: .leading)
        .onAppear {
            populateTimerangeFields(from: text.wrappedValue)
        }
    }

    private func uvRangeHelperPopover(text: Binding<String>, help: String) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(help)
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
            Picker("Units", selection: $uvRangeUnit) {
                Text("meters").tag("m")
                Text("kilo-lambda").tag("klambda")
            }
            .pickerStyle(.segmented)
            HStack {
                TextField("min", text: $uvRangeMinText)
                    .textFieldStyle(.roundedBorder)
                Text("to")
                    .foregroundStyle(.secondary)
                TextField("max", text: $uvRangeMaxText)
                    .textFieldStyle(.roundedBorder)
            }
            HStack {
                Button("Apply") {
                    applyUVRangeSelection(text: text)
                }
                Button("Clear") {
                    uvRangeMinText = ""
                    uvRangeMaxText = ""
                    text.wrappedValue = ""
                }
                Spacer()
                Button(isScanningUVRange ? "Scanning..." : "Scan MS") {
                    scanUVRange(text: text)
                }
                .disabled(isScanningUVRange)
            }
            if let uvRangeScanStatus {
                Text(uvRangeScanStatus)
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
        .padding(12)
        .frame(width: 340, alignment: .leading)
        .onAppear {
            populateUVRangeFields(from: text.wrappedValue)
        }
    }

    private func antennaHelperPopover(text: Binding<String>, help: String) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(help)
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
            HStack {
                Button("Clear") {
                    text.wrappedValue = ""
                }
                Button("All") {
                    text.wrappedValue = ""
                }
                Spacer()
                Text(text.wrappedValue.isEmpty ? "all antennas" : text.wrappedValue)
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }
            ScrollView {
                LazyVGrid(columns: [GridItem(.adaptive(minimum: 82), spacing: 6)], alignment: .leading, spacing: 6) {
                    ForEach(dataset.antennas, id: \.self) { antenna in
                        Button(antenna) {
                            toggleAntenna(antenna, text: text)
                        }
                        .buttonStyle(.bordered)
                    }
                }
            }
            .frame(maxHeight: 220)
        }
        .padding(12)
        .frame(width: 380, alignment: .leading)
    }

    private func integerSetHelperPopover(
        label: String,
        text: Binding<String>,
        help: String,
        helperOptions: [SelectionHelperOption]
    ) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(help)
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
            HStack {
                TextField("start", text: $integerRangeStartText)
                    .textFieldStyle(.roundedBorder)
                Text("to")
                    .foregroundStyle(.secondary)
                TextField("end", text: $integerRangeEndText)
                    .textFieldStyle(.roundedBorder)
            }
            HStack {
                Button("Apply range") {
                    applyIntegerRangeSelection(label: label, text: text)
                }
                Button("Clear") {
                    integerRangeStartText = ""
                    integerRangeEndText = ""
                    text.wrappedValue = ""
                }
                Spacer()
                Text(text.wrappedValue.isEmpty ? "all \(label.lowercased())s" : text.wrappedValue)
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }
            ScrollView {
                LazyVGrid(columns: [GridItem(.adaptive(minimum: 90), spacing: 6)], alignment: .leading, spacing: 6) {
                    ForEach(helperOptions.filter { !$0.value.isEmpty }) { option in
                        Button(option.label) {
                            toggleCommaToken(option.value, text: text)
                        }
                        .buttonStyle(.bordered)
                    }
                }
            }
            .frame(maxHeight: 220)
        }
        .padding(12)
        .frame(width: 390, alignment: .leading)
        .onAppear {
            populateIntegerRangeFields(from: text.wrappedValue)
        }
    }

    private func toggleListHelperPopover(
        emptyLabel: String,
        text: Binding<String>,
        help: String,
        helperOptions: [SelectionHelperOption]
    ) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(help)
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
            HStack {
                Button("Clear") {
                    text.wrappedValue = ""
                }
                Spacer()
                Text(text.wrappedValue.isEmpty ? emptyLabel : text.wrappedValue)
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }
            ScrollView {
                VStack(alignment: .leading, spacing: 6) {
                    ForEach(helperOptions.filter { !$0.value.isEmpty }) { option in
                        Button(option.label) {
                            toggleCommaToken(option.value, text: text)
                        }
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
            }
            .frame(maxHeight: 220)
        }
        .padding(12)
        .frame(width: 360, alignment: .leading)
    }

    private func msSelectHelperPopover(text: Binding<String>, help: String) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(help)
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
            Text("This builds a simple TaQL clause. Use the text field directly for expressions beyond one column comparison.")
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
            Picker("Column", selection: $msSelectColumn) {
                ForEach(dataset.columns, id: \.self) { column in
                    Text(column).tag(column)
                }
            }
            HStack {
                Picker("Operator", selection: $msSelectOperator) {
                    ForEach(["==", "!=", ">", ">=", "<", "<="], id: \.self) { op in
                        Text(op).tag(op)
                    }
                }
                .frame(width: 92)
                TextField("value", text: $msSelectValue)
                    .textFieldStyle(.roundedBorder)
            }
            HStack {
                Button("Apply") {
                    applyMSSelect(text: text)
                }
                Button("Clear") {
                    msSelectValue = ""
                    text.wrappedValue = ""
                }
            }
        }
        .padding(12)
        .frame(width: 380, alignment: .leading)
        .onAppear {
            if msSelectColumn.isEmpty {
                msSelectColumn = dataset.columns.first ?? ""
            }
        }
    }

    private func avgChannelHelperPopover(text: Binding<String>, help: String) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(help)
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
            if let limit = selectedSpectralWindowChannelLimit {
                Text("Maximum useful bin for selected SPW: \(limit)")
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
            }
            HStack {
                TextField("channels", text: text)
                    .textFieldStyle(.roundedBorder)
                Button("Apply") {
                    applyAveragingText()
                }
                Button("Clear") {
                    text.wrappedValue = ""
                    applyAveragingText()
                }
            }
            ScrollView {
                VStack(alignment: .leading, spacing: 6) {
                    ForEach(avgChannelOptions) { option in
                        Button(option.label) {
                            text.wrappedValue = option.value
                            applyAveragingText()
                        }
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
            }
            .frame(maxHeight: 180)
        }
        .padding(12)
        .frame(width: 330, alignment: .leading)
    }

    private func avgTimeHelperPopover(text: Binding<String>, help: String) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text(help)
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
            Picker("Units", selection: $avgTimeUnit) {
                Text("seconds").tag("s")
                Text("minutes").tag("min")
                Text("hours").tag("h")
            }
            .pickerStyle(.segmented)
            HStack {
                TextField("interval", text: text)
                    .textFieldStyle(.roundedBorder)
                Button("Apply") {
                    applyAvgTimeUnit(text: text)
                }
                Button("Clear") {
                    text.wrappedValue = ""
                    applyAveragingText()
                }
            }
            ScrollView {
                VStack(alignment: .leading, spacing: 6) {
                    ForEach(avgTimeOptions) { option in
                        Button(option.label) {
                            text.wrappedValue = option.value
                            avgTimeUnit = "s"
                            applyAveragingText()
                        }
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
            }
            .frame(maxHeight: 160)
        }
        .padding(12)
        .frame(width: 340, alignment: .leading)
    }

    private func applyChannelSelection(text: Binding<String>) {
        let start = channelStartText.trimmingCharacters(in: .whitespacesAndNewlines)
        let end = channelEndText.trimmingCharacters(in: .whitespacesAndNewlines)
        let step = channelStepText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !start.isEmpty, Int(start) != nil else {
            return
        }
        let candidate: String
        if end.isEmpty {
            candidate = start
        } else if step.isEmpty || step == "1" {
            candidate = "\(start)~\(end)"
        } else {
            candidate = "\(start)~\(end)^\(step)"
        }
        if isValidChannelSelection(candidate) {
            text.wrappedValue = candidate
        }
    }

    private func populateChannelFields(from value: String) {
        let first = value
            .split(whereSeparator: { $0 == ";" || $0 == "," })
            .first
            .map(String.init) ?? ""
        guard !first.isEmpty else {
            channelStartText = ""
            channelEndText = ""
            channelStepText = ""
            return
        }
        let stepped = first.split(separator: "^", omittingEmptySubsequences: false)
        channelStepText = stepped.count == 2 ? String(stepped[1]) : ""
        let range = stepped[0].split(separator: "~", omittingEmptySubsequences: false)
        channelStartText = range.first.map(String.init) ?? ""
        channelEndText = range.count == 2 ? String(range[1]) : ""
    }

    private func applyTimerangeSelection(text: Binding<String>) {
        let start = timerangeStartText.trimmingCharacters(in: .whitespacesAndNewlines)
        let end = timerangeEndText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard (start.isEmpty || Double(start) != nil), (end.isEmpty || Double(end) != nil) else {
            timerangeScanStatus = "Enter numeric MJD seconds."
            return
        }
        if start.isEmpty && end.isEmpty {
            text.wrappedValue = ""
        } else if !start.isEmpty && !end.isEmpty {
            text.wrappedValue = "\(start)~\(end)"
        } else if !start.isEmpty {
            text.wrappedValue = ">\(start)"
        } else {
            text.wrappedValue = "<\(end)"
        }
        timerangeScanStatus = nil
    }

    private func populateTimerangeFields(from value: String) {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            timerangeStartText = ""
            timerangeEndText = ""
            return
        }
        let range = trimmed.split(separator: "~", omittingEmptySubsequences: false)
        if range.count == 2 {
            timerangeStartText = String(range[0])
            timerangeEndText = String(range[1])
        } else if trimmed.hasPrefix(">") {
            timerangeStartText = String(trimmed.drop(while: { $0 == ">" || $0 == "=" }))
            timerangeEndText = ""
        } else if trimmed.hasPrefix("<") {
            timerangeStartText = ""
            timerangeEndText = String(trimmed.drop(while: { $0 == "<" || $0 == "=" }))
        }
    }

    private func scanTimerange(text: Binding<String>) {
        isScanningTimerange = true
        timerangeScanStatus = "Scanning MAIN.TIME..."
        let datasetPath = dataset.path
        Task {
            do {
                let probe = try metadataClient.probeTimeRange(datasetPath: datasetPath)
                await MainActor.run {
                    timerangeStartText = Self.formatSeconds(probe.minSeconds)
                    timerangeEndText = Self.formatSeconds(probe.maxSeconds)
                    timerangeScanStatus = "Scanned \(probe.rowCount) rows. Press Apply to use these bounds."
                    isScanningTimerange = false
                }
            } catch {
                await MainActor.run {
                    timerangeScanStatus = "Time scan failed: \(error.localizedDescription)"
                    isScanningTimerange = false
                }
            }
        }
    }

    private func applyIntegerRangeSelection(label: String, text: Binding<String>) {
        let start = integerRangeStartText.trimmingCharacters(in: .whitespacesAndNewlines)
        let end = integerRangeEndText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !start.isEmpty, Int(start) != nil else {
            return
        }
        let candidate = end.isEmpty ? start : "\(start)~\(end)"
        let labels: [String]
        switch label {
        case "Scan":
            labels = dataset.scans
        case "Array":
            labels = dataset.arrays
        case "Observation":
            labels = dataset.observations
        case "Feed":
            labels = dataset.feeds
        default:
            labels = []
        }
        if isValidIntegerSelection(candidate, labels: labels) {
            text.wrappedValue = candidate
        }
    }

    private func populateIntegerRangeFields(from value: String) {
        let first = value
            .split(separator: ",")
            .first
            .map(String.init) ?? ""
        let range = first.split(separator: "~", omittingEmptySubsequences: false)
        integerRangeStartText = range.first.map(String.init) ?? ""
        integerRangeEndText = range.count == 2 ? String(range[1]) : ""
    }

    private func applyMSSelect(text: Binding<String>) {
        let value = msSelectValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !msSelectColumn.isEmpty, !value.isEmpty else {
            return
        }
        let renderedValue: String
        if Double(value) != nil || value.uppercased() == "TRUE" || value.uppercased() == "FALSE" {
            renderedValue = value
        } else {
            renderedValue = "'\(value.replacingOccurrences(of: "'", with: "\\'"))'"
        }
        text.wrappedValue = "\(msSelectColumn) \(msSelectOperator) \(renderedValue)"
    }

    private func applyAvgTimeUnit(text: Binding<String>) {
        let rawValue = text.wrappedValue.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let value = Double(rawValue), value.isFinite, value > 0 else {
            applyAveragingText()
            return
        }
        let seconds: Double
        switch avgTimeUnit {
        case "min":
            seconds = value * 60.0
        case "h":
            seconds = value * 3_600.0
        default:
            seconds = value
        }
        text.wrappedValue = Self.formatSeconds(seconds)
        applyAveragingText()
    }

    private func applyUVRangeSelection(text: Binding<String>) {
        let minValue = uvRangeMinText.trimmingCharacters(in: .whitespacesAndNewlines)
        let maxValue = uvRangeMaxText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard (minValue.isEmpty || Double(minValue) != nil),
              (maxValue.isEmpty || Double(maxValue) != nil) else {
            uvRangeScanStatus = "Enter numeric UV bounds."
            return
        }
        if minValue.isEmpty && maxValue.isEmpty {
            text.wrappedValue = ""
        } else if !minValue.isEmpty && !maxValue.isEmpty {
            text.wrappedValue = "\(minValue)~\(maxValue)\(uvRangeUnit)"
        } else if !minValue.isEmpty {
            text.wrappedValue = ">\(minValue)\(uvRangeUnit)"
        } else {
            text.wrappedValue = "<\(maxValue)\(uvRangeUnit)"
        }
        uvRangeScanStatus = nil
    }

    private func populateUVRangeFields(from value: String) {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return
        }
        let unit = trimmed.lowercased().hasSuffix("klambda") ? "klambda" : "m"
        uvRangeUnit = unit
        var body = trimmed
        if body.lowercased().hasSuffix("klambda") {
            body.removeLast("klambda".count)
        } else if body.lowercased().hasSuffix("m") {
            body.removeLast()
        }
        let range = body.split(separator: "~", omittingEmptySubsequences: false)
        if range.count == 2 {
            uvRangeMinText = String(range[0])
            uvRangeMaxText = String(range[1])
        } else if body.hasPrefix(">") {
            uvRangeMinText = String(body.drop(while: { $0 == ">" || $0 == "=" }))
            uvRangeMaxText = ""
        } else if body.hasPrefix("<") {
            uvRangeMinText = ""
            uvRangeMaxText = String(body.drop(while: { $0 == "<" || $0 == "=" }))
        }
    }

    private func scanUVRange(text: Binding<String>) {
        isScanningUVRange = true
        uvRangeScanStatus = "Scanning MAIN.UVW..."
        let datasetPath = dataset.path
        let unit = uvRangeUnit
        Task {
            do {
                let probe = try metadataClient.probeUVRange(datasetPath: datasetPath)
                await MainActor.run {
                    if unit == "klambda" {
                        uvRangeMinText = MeasurementSetUVRangeFormatter.formatKiloLambda(probe.minKiloLambda)
                        uvRangeMaxText = MeasurementSetUVRangeFormatter.formatKiloLambda(probe.maxKiloLambda)
                    } else {
                        uvRangeMinText = MeasurementSetUVRangeFormatter.formatMeters(probe.minMeters)
                        uvRangeMaxText = MeasurementSetUVRangeFormatter.formatMeters(probe.maxMeters)
                    }
                    uvRangeScanStatus = "Scanned \(probe.rowCount) rows. Press Apply to use these bounds."
                    isScanningUVRange = false
                }
            } catch {
                await MainActor.run {
                    uvRangeScanStatus = "UV scan failed: \(error.localizedDescription)"
                    isScanningUVRange = false
                }
            }
        }
    }

    private func toggleAntenna(_ antenna: String, text: Binding<String>) {
        var tokens = text.wrappedValue
            .split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
        if let index = tokens.firstIndex(of: antenna) {
            tokens.remove(at: index)
        } else {
            tokens.append(antenna)
        }
        text.wrappedValue = tokens.joined(separator: ",")
    }

    private func toggleCommaToken(_ token: String, text: Binding<String>) {
        var tokens = text.wrappedValue
            .split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .filter { !$0.isEmpty }
        if let index = tokens.firstIndex(of: token) {
            tokens.remove(at: index)
        } else {
            tokens.append(token)
        }
        text.wrappedValue = tokens.joined(separator: ",")
    }

    private var selectedSpectralWindowChannelLimit: Int? {
        let counts = dataset.spectralWindows.compactMap(Self.channelCount(fromSpectralWindowLabel:))
        guard !counts.isEmpty else {
            return nil
        }
        guard let selected = plotState.selectedSpectralWindow, selected != "all" else {
            return counts.min()
        }
        return Self.channelCount(fromSpectralWindowLabel: selected)
    }

    private var channelSelectionOptions: [SelectionHelperOption] {
        guard let channelLimit = selectedSpectralWindowChannelLimit, channelLimit > 0 else {
            return [SelectionHelperOption(label: "All channels", value: "")]
        }
        var options = [SelectionHelperOption(label: "All channels", value: "")]
        options.append(SelectionHelperOption(label: "First channel", value: "0"))
        if channelLimit > 1 {
            options.append(SelectionHelperOption(label: "All explicit", value: "0~\(channelLimit - 1)"))
        }
        if channelLimit >= 8 {
            options.append(SelectionHelperOption(label: "First eight", value: "0~7"))
        }
        if channelLimit >= 16 {
            options.append(SelectionHelperOption(label: "Every fourth", value: "0~\(channelLimit - 1)^4"))
        }
        return options
    }

    private var antennaSelectionOptions: [SelectionHelperOption] {
        var options = [SelectionHelperOption(label: "All antennas", value: "")]
        options.append(contentsOf: dataset.antennas.map { name in
            SelectionHelperOption(label: name, value: name)
        })
        if dataset.antennas.count >= 2 {
            options.append(
                SelectionHelperOption(
                    label: "\(dataset.antennas[0]) & \(dataset.antennas[1])",
                    value: "\(dataset.antennas[0])&\(dataset.antennas[1])"
                )
            )
        }
        return options
    }

    private var scanSelectionOptions: [SelectionHelperOption] {
        integerSelectionOptions(title: "All scans", labels: dataset.scans)
    }

    private var arraySelectionOptions: [SelectionHelperOption] {
        integerSelectionOptions(title: "All arrays", labels: dataset.arrays)
    }

    private var observationSelectionOptions: [SelectionHelperOption] {
        integerSelectionOptions(title: "All observations", labels: dataset.observations)
    }

    private var intentSelectionOptions: [SelectionHelperOption] {
        [SelectionHelperOption(label: "All intents", value: "")]
            + dataset.intents.map { SelectionHelperOption(label: $0, value: $0) }
    }

    private var feedSelectionOptions: [SelectionHelperOption] {
        integerSelectionOptions(title: "All feeds", labels: dataset.feeds)
    }

    private var avgChannelOptions: [SelectionHelperOption] {
        let limit = selectedSpectralWindowChannelLimit ?? 0
        var options = [SelectionHelperOption(label: "No channel averaging", value: "")]
        for value in [2, 4, 8, 16, 32, 64] where limit == 0 || value <= limit {
            options.append(SelectionHelperOption(label: "\(value) channels", value: "\(value)"))
        }
        if limit > 0 {
            options.append(SelectionHelperOption(label: "Whole selected SPW", value: "\(limit)"))
        }
        return options
    }

    private var avgTimeOptions: [SelectionHelperOption] {
        [
            SelectionHelperOption(label: "No time averaging", value: ""),
            SelectionHelperOption(label: "10 seconds", value: "10"),
            SelectionHelperOption(label: "30 seconds", value: "30"),
            SelectionHelperOption(label: "60 seconds", value: "60"),
            SelectionHelperOption(label: "5 minutes", value: "300")
        ]
    }

    private func integerSelectionOptions(title: String, labels: [String]) -> [SelectionHelperOption] {
        [SelectionHelperOption(label: title, value: "")]
            + labels.map { label in
                SelectionHelperOption(label: label, value: Self.lastIntegerToken(in: label).map(String.init) ?? label)
            }
    }

    private func isValidChannelSelection(_ value: String) -> Bool {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return true
        }
        guard let channelLimit = selectedSpectralWindowChannelLimit, channelLimit > 0 else {
            return false
        }
        let tokens = trimmed
            .split(whereSeparator: { $0 == ";" || $0 == "," })
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
        guard !tokens.isEmpty else {
            return false
        }
        return tokens.allSatisfy { token in
            let stepped = token.split(separator: "^", omittingEmptySubsequences: false)
            guard stepped.count <= 2 else {
                return false
            }
            if stepped.count == 2 {
                guard let step = Int(stepped[1]), step > 0 else {
                    return false
                }
            }
            let rangeParts = stepped[0].split(separator: "~", omittingEmptySubsequences: false)
            if rangeParts.count == 1, let channel = Int(rangeParts[0]) {
                return channel >= 0 && channel < channelLimit
            }
            guard rangeParts.count == 2,
                  let start = Int(rangeParts[0]),
                  let end = Int(rangeParts[1]) else {
                return false
            }
            return start >= 0 && start <= end && end < channelLimit
        }
    }

    private func isValidIntegerSelection(_ value: String, labels: [String]) -> Bool {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return true
        }
        let validIDs = Set(labels.compactMap(Self.lastIntegerToken(in:)))
        guard !validIDs.isEmpty else {
            return false
        }
        let tokens = trimmed
            .split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
        guard !tokens.isEmpty else {
            return false
        }
        return tokens.allSatisfy { token in
            let rangeParts = token.split(separator: "~", omittingEmptySubsequences: false)
            if rangeParts.count == 1, let id = Int(rangeParts[0]) {
                return validIDs.contains(id)
            }
            guard rangeParts.count == 2,
                  let start = Int(rangeParts[0]),
                  let end = Int(rangeParts[1]),
                  start <= end else {
                return false
            }
            return (start...end).allSatisfy(validIDs.contains)
        }
    }

    private func isValidAntennaSelection(_ value: String) -> Bool {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return true
        }
        let validNames = Set(dataset.antennas)
        guard !validNames.isEmpty else {
            return false
        }
        let tokens = trimmed
            .split(separator: ",")
            .flatMap { $0.split(separator: "&") }
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
        guard !tokens.isEmpty else {
            return false
        }
        return tokens.allSatisfy { token in
            if validNames.contains(token) {
                return true
            }
            if let id = Int(token), id >= 0, id < dataset.antennas.count {
                return true
            }
            return false
        }
    }

    private func isValidUVRangeSelection(_ value: String) -> Bool {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return true
        }
        return trimmed
            .split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .allSatisfy(isValidUVRangePart)
    }

    private func isValidTimerangeSelection(_ value: String) -> Bool {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return true
        }
        return trimmed
            .split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .allSatisfy(isValidNumericRangePart)
    }

    private func isValidNumericRangePart(_ value: String) -> Bool {
        for prefix in [">=", "<=", ">", "<"] where value.hasPrefix(prefix) {
            return Double(value.dropFirst(prefix.count)) != nil
        }
        let range = value.split(separator: "~", omittingEmptySubsequences: false)
        if range.count == 2 {
            guard let start = Double(range[0]), let end = Double(range[1]) else {
                return false
            }
            return start <= end
        }
        return Double(value) != nil
    }

    private func isValidUVRangePart(_ value: String) -> Bool {
        for prefix in [">=", "<=", ">", "<"] where value.hasPrefix(prefix) {
            return Self.parseUVBound(String(value.dropFirst(prefix.count))) != nil
        }
        let range = value.split(separator: "~", omittingEmptySubsequences: false)
        if range.count == 2 {
            guard let start = Self.parseUVBound(String(range[0])),
                  let end = Self.parseUVBound(String(range[1])) else {
                return false
            }
            if let startUnit = start.unit, let endUnit = end.unit, startUnit != endUnit {
                return false
            }
            return start.value <= end.value
        }
        return Self.parseUVBound(value) != nil
    }

    private static func parseUVBound(_ value: String) -> (value: Double, unit: String?)? {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return nil
        }
        let splitIndex = trimmed.firstIndex { character in
            !(character.isNumber || character == "." || character == "+" || character == "-")
        } ?? trimmed.endIndex
        let numberText = String(trimmed[..<splitIndex])
        let unitText = trimmed[splitIndex...].trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        guard let number = Double(numberText), number.isFinite else {
            return nil
        }
        if unitText.isEmpty {
            return (number, nil)
        }
        guard ["m", "lambda", "klambda", "mlambda", "glambda"].contains(unitText) else {
            return nil
        }
        return (number, unitText)
    }

    private func isValidIntentSelection(_ value: String) -> Bool {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return true
        }
        let validIntents = Set(dataset.intents)
        guard !validIntents.isEmpty else {
            return false
        }
        return trimmed
            .split(separator: ",")
            .map { $0.trimmingCharacters(in: .whitespacesAndNewlines) }
            .allSatisfy(validIntents.contains)
    }

    private func isValidAvgChannelText(_ value: String) -> Bool {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return true
        }
        guard let avgChannel = UInt64(trimmed), avgChannel > 0 else {
            return false
        }
        guard let channelLimit = selectedSpectralWindowChannelLimit else {
            return true
        }
        return avgChannel <= UInt64(channelLimit)
    }

    private func isValidPositiveSecondsText(_ value: String) -> Bool {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return true
        }
        guard let seconds = Double(trimmed) else {
            return false
        }
        return seconds.isFinite && seconds > 0
    }

    private static func channelCount(fromSpectralWindowLabel label: String) -> Int? {
        guard let colon = label.firstIndex(of: ":") else {
            return nil
        }
        let tail = label[label.index(after: colon)...].trimmingCharacters(in: .whitespaces)
        guard let channelText = tail.split(separator: " ").first else {
            return nil
        }
        return Int(channelText)
    }

    private static func lastIntegerToken(in label: String) -> Int? {
        let digits = label
            .split(whereSeparator: { !$0.isNumber })
            .last
        return digits.flatMap { Int($0) }
    }

    private static func formatSeconds(_ value: Double) -> String {
        guard value.isFinite else {
            return ""
        }
        if abs(value) >= 1_000 {
            return String(format: "%.3f", value)
        }
        return String(format: "%.6g", value)
    }

    private func selectorToken(_ value: String?) -> String? {
        guard let value = normalizedPickerValue(value) else {
            return nil
        }
        if let colon = value.firstIndex(of: ":") {
            return String(value[..<colon]).trimmingCharacters(in: .whitespacesAndNewlines)
        }
        return value
    }

    private func spectralWindowSelectorToken(_ plotState: MeasurementSetExplorerPlotState) -> String? {
        guard let spectralWindow = selectorToken(plotState.selectedSpectralWindow) else {
            return nil
        }
        guard let channelSelection = plotState.selectedChannelSelection, !channelSelection.isEmpty else {
            return spectralWindow
        }
        return "\(spectralWindow):\(channelSelection)"
    }

    private func normalizedPickerValue(_ value: String?) -> String? {
        let normalized = value?.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let normalized, !normalized.isEmpty, normalized != "all" else {
            return nil
        }
        return normalized
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
                WorkbenchPlotView(
                    plot: result.plotDocument,
                    displayModeOverride: plotDisplayMode,
                    characterSizeOverride: plotCharacterSizeOverride
                )
                    .id(result.plotDocument.dataFingerprint)
                    .contextMenu {
                        Button("Plot Controls") {
                            showingPlotControls = true
                        }
                    }
                    .popover(isPresented: $showingPlotControls, arrowEdge: .trailing) {
                        plotControls
                            .padding(16)
                            .frame(width: 340)
                    }
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

    private var plotControls: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Plot Controls")
                .workbenchFont(.headline)

            Picker("Rendering", selection: $plotDisplayMode) {
                ForEach(WorkbenchPlotDisplayMode.allCases) { mode in
                    Text(mode.controlLabel).tag(mode)
                }
            }
            .pickerStyle(.segmented)
            .accessibilityIdentifier("msPlot.displayMode.\(dataset.id)")

            SliderRow(
                title: "Character size",
                value: plotCharacterSizeOverride ?? workbenchFontSize,
                range: WorkbenchState.minimumInterfaceFontSize...WorkbenchState.maximumInterfaceFontSize,
                format: "%.0f pt"
            ) { value in
                plotCharacterSizeOverride = value
            }
            .accessibilityIdentifier("msPlot.characterSize.\(dataset.id)")

            Button("Reset Character Size") {
                plotCharacterSizeOverride = nil
            }
            .disabled(plotCharacterSizeOverride == nil)
            .accessibilityIdentifier("msPlot.characterSizeReset.\(dataset.id)")
        }
    }

    private static func formatPointBudget(_ points: UInt64) -> String {
        if points >= 1_000_000 {
            return String(format: "%.2gM", Double(points) / 1_000_000.0)
        }
        if points >= 1_000 {
            return String(format: "%.0fk", Double(points) / 1_000.0)
        }
        return "\(points)"
    }

    private var maxPlotPointsTextIsWarning: Bool {
        guard let maxPlotPoints = WorkbenchState.parseMeasurementSetPlotMaxPoints(maxPlotPointsText) else {
            return plotState.maxPlotPoints > WorkbenchState.warningMeasurementSetPlotMaxPoints
        }
        return maxPlotPoints > WorkbenchState.warningMeasurementSetPlotMaxPoints
    }

    private func applyMaxPlotPointsText() {
        guard let maxPlotPoints = WorkbenchState.parseMeasurementSetPlotMaxPoints(maxPlotPointsText) else {
            maxPlotPointsText = Self.formatPointBudget(plotState.maxPlotPoints)
            return
        }
        store.setMeasurementSetPlotMaxPoints(maxPlotPoints, datasetID: dataset.id)
        let clamped = store.state.measurementSetPlots[dataset.id]?.maxPlotPoints ?? plotState.maxPlotPoints
        maxPlotPointsText = Self.formatPointBudget(clamped)
    }

    private func applyAveragingText() {
        let trimmedAvgChannel = avgChannelText.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmedAvgChannel.isEmpty {
            store.setMeasurementSetPlotAvgChannel(nil, datasetID: dataset.id)
        } else if isValidAvgChannelText(trimmedAvgChannel), let avgChannel = UInt64(trimmedAvgChannel) {
            store.setMeasurementSetPlotAvgChannel(avgChannel, datasetID: dataset.id)
            avgChannelText = String(avgChannel)
        } else {
            avgChannelText = plotState.avgChannel.map { String($0) } ?? ""
        }

        let trimmedAvgTime = avgTimeText.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmedAvgTime.isEmpty {
            store.setMeasurementSetPlotAvgTime(nil, datasetID: dataset.id)
        } else if isValidPositiveSecondsText(trimmedAvgTime), let avgTime = Double(trimmedAvgTime) {
            store.setMeasurementSetPlotAvgTime(avgTime, datasetID: dataset.id)
            avgTimeText = String(avgTime)
        } else {
            avgTimeText = plotState.avgTime.map { String($0) } ?? ""
        }
    }
}

struct TaskPanel: View {
    @ObservedObject var store: WorkbenchStore
    let tab: WorkbenchTab

    var body: some View {
        if store.state.isDemoProject {
            fixtureTaskBody
        } else {
            GenericTaskPanel(store: store, tabID: tab.id)
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
                    TaskCatalogBlock(
                        tasks: store.state.taskCatalog,
                        activeTaskID: "calibrate",
                        categoryFilter: .constant(.all),
                        searchText: .constant("")
                    )
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
            RunProgressBar(progress: store.state.taskRun.progress)
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

private struct ImagerProgressDashboard: View {
    let snapshot: ImagerProgressSnapshot
    @State private var currentTime = Date()

    private let columns = [
        GridItem(.adaptive(minimum: 300), spacing: 14, alignment: .top)
    ]

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack(alignment: .firstTextBaseline) {
                VStack(alignment: .leading, spacing: 2) {
                    Text("Imager Progress")
                        .workbenchFont(.headline)
                    Text(snapshot.summary)
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                }
                Spacer()
                HStack(spacing: 8) {
                    ImagerProgressStateBadge(state: snapshot.state)
                    Text(snapshot.elapsedLabel(now: currentTime))
                        .workbenchFont(.caption, design: .monospaced)
                        .foregroundStyle(.secondary)
                }
            }

            HStack(alignment: .center, spacing: 10) {
                Text(snapshot.phase)
                    .workbenchFont(.subheadline, weight: .semibold)
                    .lineLimit(2)
                    .minimumScaleFactor(0.8)
                Spacer()
                Text(snapshot.source)
                    .workbenchFont(.caption, design: .monospaced)
                    .foregroundStyle(.secondary)
            }

            ImagerProgressStatusStrip(snapshot: snapshot)

            LazyVGrid(columns: columns, alignment: .leading, spacing: 14) {
                ImagerProgressSection(
                    title: snapshot.sourceStreamIsActive ? "MS Read Window" : "Last MS Read Window",
                    subtitle: snapshot.measurementSetWindow.rangeLabel
                ) {
                    MeasurementSetReadWindowView(window: snapshot.measurementSetWindow)
                }

                ImagerProgressSection(
                    title: "Output Cube",
                    subtitle: snapshot.outputCube.aspectLabel
                ) {
                    OutputCubeProgressView(cube: snapshot.outputCube)
                }

                ImagerProgressSection(
                    title: "Buffer Activity",
                    subtitle: "\(snapshot.resourceActivities.filter(\.isBusy).count) busy / \(snapshot.resourceActivities.count) tracked"
                ) {
                    ResourceActivityFlowView(snapshot: snapshot)
                }

                ImagerProgressSection(
                    title: "UV Coverage",
                    subtitle: "\(decimalCountLabel(UInt64(snapshot.uvCoverage.retainedMeasuredPointCount))) retained / \(decimalCountLabel(snapshot.uvCoverage.acceptedMeasuredPointCount)) gridable"
                ) {
                    UVCoverageProgressView(coverage: snapshot.uvCoverage)
                }

                ImagerProgressSection(
                    title: "Cycles",
                    subtitle: snapshot.deconvolution.phase
                ) {
                    DeconvolutionProgressView(progress: snapshot.deconvolution)
                }

                ImagerProgressSection(
                    title: "Runtime",
                    subtitle: snapshot.runtime.backend
                ) {
                    RuntimeProgressView(runtime: snapshot.runtime)
                }
            }
        }
        .accessibilityIdentifier("task.imagerProgress")
        .onReceive(Timer.publish(every: 1.0, on: .main, in: .common).autoconnect()) { date in
            if snapshot.state == .running {
                currentTime = date
            }
        }
    }
}

private struct ImagerProgressStateBadge: View {
    let state: TaskRunState

    private var isActive: Bool {
        state == .running
    }

    private var label: String {
        isActive ? "active" : "inactive: \(state.rawValue)"
    }

    private var tint: Color {
        switch state {
        case .running:
            return .green
        case .succeeded, .completed:
            return .blue
        case .failed:
            return .red
        case .cancelled, .stopped:
            return .orange
        case .idle:
            return .secondary
        }
    }

    var body: some View {
        Text(label)
            .workbenchFont(.caption2, weight: .semibold)
            .foregroundStyle(tint)
            .lineLimit(1)
            .padding(.horizontal, 7)
            .padding(.vertical, 3)
            .background(tint.opacity(isActive ? 0.16 : 0.10))
            .clipShape(Capsule())
            .overlay(
                Capsule()
                    .stroke(tint.opacity(isActive ? 0.34 : 0.24), lineWidth: 1)
            )
            .accessibilityLabel(label)
    }
}

private struct ImagerProgressStatusStrip: View {
    let snapshot: ImagerProgressSnapshot

    private let columns = [
        GridItem(.adaptive(minimum: 132), spacing: 10, alignment: .top)
    ]

    var body: some View {
        LazyVGrid(columns: columns, alignment: .leading, spacing: 10) {
            ImagerProgressStatusChip(
                title: "Work",
                value: percentLabel(snapshot.workEstimate.fraction),
                detail: snapshot.workEstimate.confidence,
                systemImage: "chart.bar.fill",
                accent: .cyan
            )
            ImagerProgressStatusChip(
                title: snapshot.sourceStreamIsActive ? "MS rows" : "Last MS rows",
                value: snapshot.measurementSetWindow.activeRowPercentLabel,
                detail: "channels \(snapshot.measurementSetWindow.activeChannelPercentLabel)",
                systemImage: "tablecells",
                accent: .blue
            )
            ImagerProgressStatusChip(
                title: "Cube planes",
                value: snapshot.outputCube.activePlanePercentLabel,
                detail: "\(snapshot.outputCube.activePlaneCount) active",
                systemImage: "cube",
                accent: .indigo
            )
            ImagerProgressStatusChip(
                title: "Clean",
                value: percentLabel(snapshot.deconvolution.minorIterationFraction),
                detail: "major \(snapshot.deconvolution.majorCycleLabel)",
                systemImage: "sparkles",
                accent: .orange
            )
            ImagerProgressStatusChip(
                title: "Runtime",
                value: snapshot.runtime.threadPercentLabel,
                detail: snapshot.runtime.gpuActive ? "GPU active" : "GPU idle",
                systemImage: snapshot.runtime.gpuActive ? "bolt.fill" : "bolt.slash",
                accent: snapshot.runtime.gpuActive ? .green : .secondary
            )
        }
        .accessibilityIdentifier("task.imagerProgress.status")
    }
}

private struct ImagerProgressStatusChip: View {
    let title: String
    let value: String
    let detail: String
    let systemImage: String
    let accent: Color

    var body: some View {
        HStack(alignment: .top, spacing: 7) {
            Image(systemName: systemImage)
                .foregroundStyle(accent)
                .frame(width: 16)

            VStack(alignment: .leading, spacing: 2) {
                Text(title)
                    .workbenchFont(.caption2)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                Text(value)
                    .workbenchFont(.subheadline, weight: .semibold, design: .monospaced)
                    .lineLimit(1)
                    .minimumScaleFactor(0.72)
                Text(detail)
                    .workbenchFont(.caption2)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .minimumScaleFactor(0.72)
            }
        }
        .padding(.horizontal, 9)
        .padding(.vertical, 8)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(Color(nsColor: .controlBackgroundColor).opacity(0.30))
        .clipShape(RoundedRectangle(cornerRadius: 7))
        .overlay(
            RoundedRectangle(cornerRadius: 7)
                .stroke(Color.secondary.opacity(0.10), lineWidth: 1)
        )
    }
}

private struct ImagerProgressSection<Content: View>: View {
    let title: String
    let subtitle: String
    @ViewBuilder var content: Content

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            VStack(alignment: .leading, spacing: 2) {
                Text(title)
                    .workbenchFont(.subheadline, weight: .semibold)
                Text(subtitle)
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(2)
            }
            content
        }
        .padding(10)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(Color(nsColor: .controlBackgroundColor).opacity(0.45))
        .clipShape(RoundedRectangle(cornerRadius: 8))
        .overlay(
            RoundedRectangle(cornerRadius: 8)
                .stroke(Color.secondary.opacity(0.14), lineWidth: 1)
        )
    }
}

private struct MeasurementSetReadWindowView: View {
    let window: MeasurementSetReadWindowProgress

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            Canvas { context, size in
                let availableWidth = max(20, size.width - 48)
                let availableHeight = max(20, size.height - 34)
                let rectHeight = min(availableHeight, availableWidth * 2)
                let rectWidth = rectHeight / 2
                let rect = CGRect(
                    x: 34 + max(0, availableWidth - rectWidth) / 2,
                    y: 14 + max(0, availableHeight - rectHeight) / 2,
                    width: rectWidth,
                    height: rectHeight
                )
                let gridColor = Color.secondary.opacity(0.18)
                let outlineColor = Color.secondary.opacity(0.45)
                context.stroke(Path(rect), with: .color(outlineColor), lineWidth: 1)

                for index in 1..<8 {
                    let x = rect.minX + rect.width * CGFloat(index) / 8
                    var path = Path()
                    path.move(to: CGPoint(x: x, y: rect.minY))
                    path.addLine(to: CGPoint(x: x, y: rect.maxY))
                    context.stroke(path, with: .color(gridColor), lineWidth: 0.75)
                }
                for index in 1..<7 {
                    let y = rect.minY + rect.height * CGFloat(index) / 7
                    var path = Path()
                    path.move(to: CGPoint(x: rect.minX, y: y))
                    path.addLine(to: CGPoint(x: rect.maxX, y: y))
                    context.stroke(path, with: .color(gridColor), lineWidth: 0.75)
                }

                let selected = CGRect(
                    x: rect.minX + rect.width * CGFloat(window.channelStartFraction),
                    y: rect.minY + rect.height * CGFloat(window.rowStartFraction),
                    width: rect.width * CGFloat(max(0, window.channelEndFraction - window.channelStartFraction)),
                    height: rect.height * CGFloat(max(0, window.rowEndFraction - window.rowStartFraction))
                )
                context.fill(Path(selected), with: .color(Color.cyan.opacity(0.18)))
                context.stroke(Path(selected), with: .color(.cyan), lineWidth: 2)
            }
            .frame(height: 220)
            .overlay(alignment: .leading) {
                Text("Rows")
                    .workbenchFont(.caption2)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .fixedSize()
                    .rotationEffect(.degrees(-90))
                    .frame(width: 32, height: 72)
            }

            HStack {
                Text("Channels")
                    .workbenchFont(.caption2)
                    .foregroundStyle(.secondary)
                Spacer()
                Text("\(window.activeRowCount) rows x \(window.activeChannelCount) channels")
                    .workbenchFont(.caption2, design: .monospaced)
                    .foregroundStyle(.secondary)
            }
        }
    }
}

private struct OutputCubeProgressView: View {
    let cube: OutputCubeProgress

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            XYZCubeRangeWidget(cube: cube)
                .frame(height: 190)

            HStack {
                Text("Outlined sub-cube: X=all, Y=all, Z=\(cube.activePlaneStart)-\(cube.activePlaneEnd)")
                    .workbenchFont(.caption2)
                    .foregroundStyle(.secondary)
                Spacer()
                Text(cube.activeRangeLabel)
                    .workbenchFont(.caption2, design: .monospaced)
                    .foregroundStyle(.secondary)
            }
        }
    }
}

private struct XYZCubeRangeWidget: View {
    let cube: OutputCubeProgress

    var body: some View {
        Canvas { context, size in
            let projection = XYZCubeProjection(size: size, zScale: CGFloat(cube.zAxisDisplayScale))
            let fullCube = XYZCubeBox(xMin: 0, xMax: 1, yMin: 0, yMax: 1, zMin: 0, zMax: 1)
            let selectedSubcube = XYZCubeBox(
                xMin: 0,
                xMax: 1,
                yMin: 0,
                yMax: 1,
                zMin: CGFloat(cube.activePlaneStartFraction),
                zMax: CGFloat(cube.activePlaneEndFraction)
            )

            draw(box: fullCube, projection: projection, in: &context, color: .secondary.opacity(0.38), lineWidth: 1)
            fill(box: selectedSubcube, projection: projection, in: &context, color: .cyan.opacity(0.10))
            draw(box: selectedSubcube, projection: projection, in: &context, color: .cyan, lineWidth: 2.2)
            drawAxisLabels(projection: projection, in: &context)
        }
        .accessibilityLabel("XYZ cube with highlighted sub-cube covering all X and all Y over selected Z range")
    }

    private func fill(
        box: XYZCubeBox,
        projection: XYZCubeProjection,
        in context: inout GraphicsContext,
        color: Color
    ) {
        for face in box.faces {
            guard let first = face.first else { continue }
            var path = Path()
            path.move(to: projection.project(first))
            for point in face.dropFirst() {
                path.addLine(to: projection.project(point))
            }
            path.closeSubpath()
            context.fill(path, with: .color(color))
        }
    }

    private func draw(
        box: XYZCubeBox,
        projection: XYZCubeProjection,
        in context: inout GraphicsContext,
        color: Color,
        lineWidth: CGFloat
    ) {
        for edge in box.edges {
            var path = Path()
            path.move(to: projection.project(edge.0))
            path.addLine(to: projection.project(edge.1))
            context.stroke(path, with: .color(color), lineWidth: lineWidth)
        }
    }

    private func drawAxisLabels(projection: XYZCubeProjection, in context: inout GraphicsContext) {
        let origin = projection.project(XYZCubePoint(x: 0, y: 0, z: 0))
        drawAxis(from: origin, to: projection.project(XYZCubePoint(x: 1.08, y: 0, z: 0)), label: "X", in: &context)
        drawAxis(from: origin, to: projection.project(XYZCubePoint(x: 0, y: 1.08, z: 0)), label: "Y", in: &context)
        drawAxis(from: origin, to: projection.project(XYZCubePoint(x: 0, y: 0, z: 1.08)), label: "Z", in: &context)
    }

    private func drawAxis(from start: CGPoint, to end: CGPoint, label: String, in context: inout GraphicsContext) {
        var path = Path()
        path.move(to: start)
        path.addLine(to: end)
        context.stroke(path, with: .color(.secondary.opacity(0.55)), lineWidth: 1)
        context.draw(
            Text(label).font(.caption2).foregroundColor(.secondary),
            at: CGPoint(x: end.x + 12, y: end.y)
        )
    }
}

private struct XYZCubeProjection {
    let origin: CGPoint
    let xAxis: CGVector
    let yAxis: CGVector
    let zAxis: CGVector

    init(size: CGSize, zScale: CGFloat) {
        let margin: CGFloat = 18
        let zAngle = CGFloat.pi / 6
        let zCos = cos(zAngle)
        let zSin = sin(zAngle)
        let zScale = max(0.05, zScale)
        let availableWidth = max(40, size.width - margin * 2)
        let availableHeight = max(40, size.height - margin * 2)
        let axisLength = min(availableWidth / (1 + zScale * zCos), availableHeight / (1 + zScale * zSin))
        let zLength = axisLength * zScale
        let projectedWidth = axisLength + zLength * zCos
        let projectedHeight = axisLength + zLength * zSin
        let top = (size.height - projectedHeight) / 2
        origin = CGPoint(
            x: (size.width - projectedWidth) / 2,
            y: top + projectedHeight
        )
        xAxis = CGVector(dx: axisLength, dy: 0)
        yAxis = CGVector(dx: 0, dy: -axisLength)
        zAxis = CGVector(dx: zLength * zCos, dy: -zLength * zSin)
    }

    func project(_ point: XYZCubePoint) -> CGPoint {
        CGPoint(
            x: origin.x + xAxis.dx * point.x + yAxis.dx * point.y + zAxis.dx * point.z,
            y: origin.y + xAxis.dy * point.x + yAxis.dy * point.y + zAxis.dy * point.z
        )
    }
}

private struct XYZCubePoint {
    var x: CGFloat
    var y: CGFloat
    var z: CGFloat
}

private struct XYZCubeBox {
    var xMin: CGFloat
    var xMax: CGFloat
    var yMin: CGFloat
    var yMax: CGFloat
    var zMin: CGFloat
    var zMax: CGFloat

    var corners: (
        p000: XYZCubePoint,
        p100: XYZCubePoint,
        p110: XYZCubePoint,
        p010: XYZCubePoint,
        p001: XYZCubePoint,
        p101: XYZCubePoint,
        p111: XYZCubePoint,
        p011: XYZCubePoint
    ) {
        (
            XYZCubePoint(x: xMin, y: yMin, z: zMin),
            XYZCubePoint(x: xMax, y: yMin, z: zMin),
            XYZCubePoint(x: xMax, y: yMax, z: zMin),
            XYZCubePoint(x: xMin, y: yMax, z: zMin),
            XYZCubePoint(x: xMin, y: yMin, z: zMax),
            XYZCubePoint(x: xMax, y: yMin, z: zMax),
            XYZCubePoint(x: xMax, y: yMax, z: zMax),
            XYZCubePoint(x: xMin, y: yMax, z: zMax)
        )
    }

    var faces: [[XYZCubePoint]] {
        let c = corners
        return [
            [c.p000, c.p100, c.p110, c.p010],
            [c.p001, c.p101, c.p111, c.p011],
            [c.p000, c.p100, c.p101, c.p001],
            [c.p100, c.p110, c.p111, c.p101],
            [c.p110, c.p010, c.p011, c.p111],
            [c.p010, c.p000, c.p001, c.p011]
        ]
    }

    var edges: [(XYZCubePoint, XYZCubePoint)] {
        let c = corners
        return [
            (c.p000, c.p100), (c.p100, c.p110), (c.p110, c.p010), (c.p010, c.p000),
            (c.p001, c.p101), (c.p101, c.p111), (c.p111, c.p011), (c.p011, c.p001),
            (c.p000, c.p001), (c.p100, c.p101), (c.p110, c.p111), (c.p010, c.p011)
        ]
    }
}

private struct UVCoverageProgressView: View {
    let coverage: UVCoverageProgress

    var body: some View {
        Canvas { context, size in
            let side = max(20, min(size.width - 36, size.height - 28))
            let rect = CGRect(
                x: (size.width - side) / 2,
                y: 12,
                width: side,
                height: side
            )
            let center = CGPoint(x: rect.midX, y: rect.midY)
            var uAxis = Path()
            uAxis.move(to: CGPoint(x: rect.minX, y: center.y))
            uAxis.addLine(to: CGPoint(x: rect.maxX, y: center.y))
            context.stroke(uAxis, with: .color(.secondary.opacity(0.35)), lineWidth: 1)

            var vAxis = Path()
            vAxis.move(to: CGPoint(x: center.x, y: rect.minY))
            vAxis.addLine(to: CGPoint(x: center.x, y: rect.maxY))
            context.stroke(vAxis, with: .color(.secondary.opacity(0.35)), lineWidth: 1)
            context.stroke(Path(ellipseIn: rect), with: .color(.secondary.opacity(0.18)), lineWidth: 1)

            draw(points: coverage.conjugate, color: .indigo, rect: rect, in: &context)
            draw(points: coverage.measured, color: .cyan, rect: rect, in: &context)
        }
        .frame(height: 210)
        .overlay(alignment: .bottomLeading) {
            HStack(spacing: 12) {
                legendDot(color: .cyan, label: "measured sample")
                legendDot(color: .indigo, label: "conjugate")
            }
            .padding(.leading, 6)
        }
    }

    private func draw(points: [UVPoint], color: Color, rect: CGRect, in context: inout GraphicsContext) {
        let uExtent = max(1, coverage.uExtentKilolambda)
        let vExtent = max(1, coverage.vExtentKilolambda)
        for point in points {
            let x = rect.midX + rect.width * 0.5 * CGFloat(point.uKilolambda / uExtent)
            let y = rect.midY - rect.height * 0.5 * CGFloat(point.vKilolambda / vExtent)
            let radius = CGFloat(0.5)
            let pointRect = CGRect(x: x - radius, y: y - radius, width: radius * 2, height: radius * 2)
            context.fill(Path(ellipseIn: pointRect), with: .color(color.opacity(0.72)))
        }
    }

    private func legendDot(color: Color, label: String) -> some View {
        HStack(spacing: 4) {
            Circle()
                .fill(color)
                .frame(width: 7, height: 7)
            Text(label)
                .workbenchFont(.caption2)
                .foregroundStyle(.secondary)
        }
    }
}

private struct DeconvolutionProgressView: View {
    let progress: ImagingDeconvolutionProgress

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            VStack(alignment: .leading, spacing: 3) {
                HStack {
                    Text("Minor-cycle iteration progress")
                        .workbenchFont(.caption2)
                        .foregroundStyle(.secondary)
                    Spacer()
                    Text(percentLabel(progress.minorIterationFraction))
                        .workbenchFont(.caption2, design: .monospaced)
                        .foregroundStyle(.secondary)
                }
                ProgressView(value: boundedProgress(progress.minorIterationFraction))
            }
            ResidualHistoryChart(progress: progress)
                .frame(height: 76)
            HStack(spacing: 12) {
                metric("Major", progress.majorCycleLabel)
                metric("Minor updates", "\(progress.minorIterations)")
                metric("Components", "\(progress.componentsCleaned)")
            }
            HStack(spacing: 12) {
                metric("Latest peak", String(format: "%.2f mJy/beam", progress.peakResidualMilliJyPerBeam))
                metric("Target", String(format: "%.2f mJy/beam", progress.targetResidualMilliJyPerBeam))
            }
        }
        .frame(minHeight: 150, alignment: .topLeading)
    }

    private func metric(_ label: String, _ value: String) -> some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(label)
                .workbenchFont(.caption2)
                .foregroundStyle(.secondary)
            Text(value)
                .workbenchFont(.caption, design: .monospaced)
                .lineLimit(1)
                .minimumScaleFactor(0.75)
        }
        .frame(minWidth: 72, alignment: .leading)
    }
}

private struct RunProgressBar: View {
    let progress: Double

    var body: some View {
        VStack(alignment: .leading, spacing: 3) {
            HStack {
                Text("Run progress")
                    .workbenchFont(.caption2)
                    .foregroundStyle(.secondary)
                Spacer()
                Text(percentLabel(progress))
                    .workbenchFont(.caption2, design: .monospaced)
                    .foregroundStyle(.secondary)
            }
            ProgressView(value: boundedProgress(progress))
            Text("Task runner progress signal")
                .workbenchFont(.caption2)
                .foregroundStyle(.secondary)
        }
    }
}

private struct WorkEstimateBar: View {
    let estimate: ImagingWorkEstimate

    var body: some View {
        VStack(alignment: .leading, spacing: 3) {
            HStack {
                Text("Estimated work")
                    .workbenchFont(.caption2)
                    .foregroundStyle(.secondary)
                Spacer()
                Text(percentLabel(estimate.fraction))
                    .workbenchFont(.caption2, design: .monospaced)
                    .foregroundStyle(.secondary)
            }
            ProgressView(value: boundedProgress(estimate.fraction))
            HStack(alignment: .firstTextBaseline) {
                Text(estimate.basis)
                    .workbenchFont(.caption2)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .minimumScaleFactor(0.75)
                Spacer()
                Text(estimate.unitsLabel)
                    .workbenchFont(.caption2, design: .monospaced)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .minimumScaleFactor(0.75)
            }
        }
    }
}

private func boundedProgress(_ progress: Double) -> Double {
    min(1, max(0, progress))
}

private func percentLabel(_ progress: Double) -> String {
    "\(Int((boundedProgress(progress) * 100).rounded()))%"
}

private func percentRangeLabel(start: Double, end: Double) -> String {
    let startPercent = Int((boundedProgress(start) * 100).rounded())
    let endPercent = Int((boundedProgress(end) * 100).rounded())
    return "\(startPercent)-\(endPercent)%"
}

private func decimalCountLabel(_ value: UInt64) -> String {
    if value >= 1_000_000_000 {
        return String(format: "%.1fB", Double(value) / 1_000_000_000)
    }
    if value >= 1_000_000 {
        return String(format: "%.1fM", Double(value) / 1_000_000)
    }
    if value >= 1_000 {
        return String(format: "%.1fk", Double(value) / 1_000)
    }
    return "\(value)"
}

private extension MeasurementSetReadWindowProgress {
    var activeRowPercentLabel: String {
        percentRangeLabel(start: rowStartFraction, end: rowEndFraction)
    }

    var activeChannelPercentLabel: String {
        percentRangeLabel(start: channelStartFraction, end: channelEndFraction)
    }
}

private extension OutputCubeProgress {
    var activePlanePercentLabel: String {
        percentRangeLabel(start: activePlaneStartFraction, end: activePlaneEndFraction)
    }
}

private extension ImagingRuntimeProgress {
    var threadPercentLabel: String {
        percentLabel(activeThreadFraction)
    }
}

private extension ImagingDeconvolutionProgress {
    var majorCycleLabel: String {
        guard majorCycleLimit > 0 else {
            return "\(majorCycle) / auto"
        }
        return "\(majorCycle) / \(majorCycleLimit)"
    }
}

private struct ResourceActivityFlowView: View {
    let snapshot: ImagerProgressSnapshot

    private var resources: [ImagingResourceActivity] {
        snapshot.resourceActivities
    }

    private var canvasHeight: CGFloat {
        max(190, CGFloat(max(resources.count, 1)) * 44 + 12)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Canvas { context, size in
                guard !resources.isEmpty else {
                    Self.drawEmptyState(in: &context, size: size)
                    return
                }

                let rowHeight: CGFloat = 34
                let rowGap: CGFloat = 10
                let xInset: CGFloat = 6
                let rowWidth = max(40, size.width - xInset * 2)

                for (index, resource) in resources.enumerated() {
                    let y = CGFloat(index) * (rowHeight + rowGap) + 6
                    let rowRect = CGRect(x: xInset, y: y, width: rowWidth, height: rowHeight)
                    let nextBusy = index + 1 < resources.count && resources[index + 1].isBusy
                    Self.drawResource(resource, in: rowRect, context: &context)
                    if index + 1 < resources.count {
                        Self.drawFlowArrow(
                            from: CGPoint(x: rowRect.midX, y: rowRect.maxY + 1),
                            to: CGPoint(x: rowRect.midX, y: rowRect.maxY + rowGap - 1),
                            active: resource.isBusy || nextBusy,
                            context: &context
                        )
                    }
                }
            }
            .frame(height: canvasHeight)

            HStack(spacing: 12) {
                Label("busy", systemImage: "circle")
                    .foregroundStyle(Color.cyan)
                Label("active section", systemImage: "rectangle.inset.filled")
                    .foregroundStyle(Color.green)
                Label("GPU", systemImage: "bolt.fill")
                    .foregroundStyle(Color.orange)
            }
            .workbenchFont(.caption2)
            .foregroundStyle(.secondary)
        }
    }

    private static func drawEmptyState(in context: inout GraphicsContext, size: CGSize) {
        let rect = CGRect(x: 6, y: 8, width: max(20, size.width - 12), height: max(44, size.height - 16))
        context.fill(
            Path(roundedRect: rect, cornerRadius: 6),
            with: .color(.secondary.opacity(0.06))
        )
        context.stroke(
            Path(roundedRect: rect, cornerRadius: 6),
            with: .color(.secondary.opacity(0.20)),
            style: StrokeStyle(lineWidth: 1, dash: [4, 4])
        )
        context.draw(
            Text("waiting for buffer plan").font(.caption2).foregroundColor(.secondary),
            at: CGPoint(x: rect.midX, y: rect.midY)
        )
    }

    private static func drawResource(
        _ resource: ImagingResourceActivity,
        in rect: CGRect,
        context: inout GraphicsContext
    ) {
        let tint = color(for: resource)
        let outline = resource.isBusy ? tint : Color.secondary.opacity(0.35)
        let strokeStyle = StrokeStyle(lineWidth: resource.isBusy ? 2 : 1, dash: resource.isBusy ? [] : [4, 4])
        let rowPath = Path(roundedRect: rect, cornerRadius: 6)
        context.fill(rowPath, with: .color(tint.opacity(resource.isBusy ? 0.12 : 0.045)))
        context.stroke(rowPath, with: .color(outline), style: strokeStyle)

        let metricWidth: CGFloat = 78
        let maxTextWidth = max(112, rect.width - metricWidth - 36)
        let textWidth = min(max(150, rect.width * 0.55), min(176, maxTextWidth))
        let barRect = CGRect(
            x: rect.minX + textWidth,
            y: rect.minY + 7,
            width: max(24, rect.width - textWidth - metricWidth),
            height: rect.height - 14
        )
        context.fill(
            Path(roundedRect: barRect, cornerRadius: 4),
            with: .color(.secondary.opacity(0.09))
        )

        let sectionWidth = barRect.width * CGFloat(max(0, resource.sectionEndFraction - resource.sectionStartFraction))
        if sectionWidth > 0 {
            let sectionRect = CGRect(
                x: barRect.minX + barRect.width * CGFloat(resource.sectionStartFraction),
                y: barRect.minY,
                width: max(3, min(sectionWidth, barRect.maxX - barRect.minX)),
                height: barRect.height
            )
            context.fill(
                Path(roundedRect: sectionRect, cornerRadius: 4),
                with: .color(tint.opacity(resource.isBusy ? 0.42 : 0.20))
            )
        }

        let byteWidth = barRect.width * CGFloat(resource.byteFraction)
        if byteWidth > 0 {
            let byteRect = CGRect(
                x: barRect.minX,
                y: barRect.maxY - 3,
                width: max(2, byteWidth),
                height: 3
            )
            context.fill(Path(byteRect), with: .color(tint.opacity(0.75)))
        }

        context.draw(
            Text(resource.name).font(.caption2.weight(.semibold)).foregroundColor(.primary),
            at: CGPoint(x: rect.minX + 8, y: rect.midY - 6),
            anchor: .leading
        )
        context.draw(
            Text(resource.detail).font(.caption2).foregroundColor(.secondary),
            at: CGPoint(x: rect.minX + 8, y: rect.midY + 8),
            anchor: .leading
        )

        drawThreadDots(resource, in: rect, context: &context)

        if resource.gpuActive {
            let gpuRect = CGRect(x: rect.maxX - 18, y: rect.midY - 7, width: 14, height: 14)
            context.fill(Path(ellipseIn: gpuRect), with: .color(.orange.opacity(0.25)))
            context.draw(
                Text("G").font(.caption2.weight(.bold)).foregroundColor(.orange),
                at: CGPoint(x: gpuRect.midX, y: gpuRect.midY)
            )
        }
    }

    private static func drawThreadDots(
        _ resource: ImagingResourceActivity,
        in rect: CGRect,
        context: inout GraphicsContext
    ) {
        let dotCount = min(max(resource.totalThreads, 0), 8)
        guard dotCount > 0 else { return }

        let activeDots = min(
            dotCount,
            Int(ceil(Double(dotCount) * Double(resource.activeThreads) / Double(max(resource.totalThreads, 1))))
        )
        let startX = rect.maxX - 70
        for index in 0..<dotCount {
            let x = startX + CGFloat(index % 4) * 7
            let y = rect.midY - 7 + CGFloat(index / 4) * 8
            let dot = CGRect(x: x, y: y, width: 5, height: 5)
            let color = index < activeDots ? Color.cyan.opacity(0.9) : Color.secondary.opacity(0.22)
            context.fill(Path(ellipseIn: dot), with: .color(color))
        }
        context.draw(
            Text("\(resource.activeThreads)/\(resource.totalThreads)").font(.caption2).foregroundColor(.secondary),
            at: CGPoint(x: rect.maxX - 29, y: rect.midY),
            anchor: .trailing
        )
    }

    private static func drawFlowArrow(
        from start: CGPoint,
        to end: CGPoint,
        active: Bool,
        context: inout GraphicsContext
    ) {
        let color = active ? Color.cyan.opacity(0.65) : Color.secondary.opacity(0.22)
        var path = Path()
        path.move(to: start)
        path.addLine(to: end)
        context.stroke(path, with: .color(color), style: StrokeStyle(lineWidth: active ? 1.4 : 1, lineCap: .round))

        var head = Path()
        head.move(to: end)
        head.addLine(to: CGPoint(x: end.x - 4, y: end.y - 4))
        head.move(to: end)
        head.addLine(to: CGPoint(x: end.x + 4, y: end.y - 4))
        context.stroke(head, with: .color(color), style: StrokeStyle(lineWidth: active ? 1.4 : 1, lineCap: .round))
    }

    private static func color(for resource: ImagingResourceActivity) -> Color {
        switch resource.kind {
        case .source:
            return .cyan
        case .grid:
            return .blue
        case .plane:
            return .indigo
        case .deconvolver:
            return .green
        case .product:
            return .orange
        }
    }
}

private struct ResidualHistoryChart: View {
    let progress: ImagingDeconvolutionProgress

    var body: some View {
        VStack(alignment: .leading, spacing: 3) {
            Text(progress.residualHistoryMilliJyPerBeam.count > 1 ? "Recent residual peaks" : "Current residual peak")
                .workbenchFont(.caption2)
                .foregroundStyle(.secondary)
            Canvas { context, size in
                let rect = CGRect(x: 4, y: 4, width: max(20, size.width - 8), height: max(20, size.height - 8))
                context.stroke(Path(rect), with: .color(.secondary.opacity(0.18)), lineWidth: 1)

                let values = progress.residualHistoryMilliJyPerBeam.isEmpty
                    ? [progress.peakResidualMilliJyPerBeam]
                    : progress.residualHistoryMilliJyPerBeam
                let maxValue = max(values.max() ?? progress.peakResidualMilliJyPerBeam, progress.targetResidualMilliJyPerBeam, 0.001)
                let minValue = 0.0
                let targetY = yPosition(progress.targetResidualMilliJyPerBeam, minValue: minValue, maxValue: maxValue, rect: rect)
                var target = Path()
                target.move(to: CGPoint(x: rect.minX, y: targetY))
                target.addLine(to: CGPoint(x: rect.maxX, y: targetY))
                context.stroke(target, with: .color(.green.opacity(0.65)), style: StrokeStyle(lineWidth: 1, dash: [3, 3]))

                var line = Path()
                for (index, value) in values.enumerated() {
                    let x = values.count == 1
                        ? rect.midX
                        : rect.minX + rect.width * CGFloat(index) / CGFloat(values.count - 1)
                    let y = yPosition(value, minValue: minValue, maxValue: maxValue, rect: rect)
                    if index == 0 {
                        line.move(to: CGPoint(x: x, y: y))
                    } else {
                        line.addLine(to: CGPoint(x: x, y: y))
                    }
                }
                if values.count == 1 {
                    let y = yPosition(values[0], minValue: minValue, maxValue: maxValue, rect: rect)
                    context.fill(
                        Path(ellipseIn: CGRect(x: rect.midX - 2, y: y - 2, width: 4, height: 4)),
                        with: .color(.cyan)
                    )
                } else {
                    context.stroke(line, with: .color(.cyan), lineWidth: 1.5)
                }
            }
        }
    }

    private func yPosition(_ value: Double, minValue: Double, maxValue: Double, rect: CGRect) -> CGFloat {
        guard maxValue > minValue else { return rect.midY }
        let fraction = min(1, max(0, (value - minValue) / (maxValue - minValue)))
        return rect.maxY - rect.height * CGFloat(fraction)
    }
}

private struct RuntimeProgressView: View {
    let runtime: ImagingRuntimeProgress

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack(alignment: .firstTextBaseline) {
                Text("\(runtime.activeThreads) / \(runtime.totalThreads) threads")
                    .workbenchFont(.subheadline, weight: .semibold)
                Spacer()
                Label(runtime.gpuActive ? "GPU active" : "GPU idle", systemImage: runtime.gpuActive ? "bolt.fill" : "bolt.slash")
                    .workbenchFont(.caption)
                    .foregroundStyle(runtime.gpuActive ? Color.green : Color.secondary)
            }
            LazyVGrid(columns: Array(repeating: GridItem(.fixed(18), spacing: 4), count: 8), alignment: .leading, spacing: 5) {
                ForEach(0..<max(0, runtime.totalThreads), id: \.self) { index in
                    RoundedRectangle(cornerRadius: 3)
                        .fill(index < runtime.activeThreads ? Color.cyan.opacity(0.85) : Color.secondary.opacity(0.22))
                        .frame(width: 18, height: 18)
                        .accessibilityLabel("Thread \(index + 1)")
                }
            }
            Text(runtime.sampleCadence)
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
        }
        .frame(minHeight: 150, alignment: .topLeading)
    }
}

private struct TaskParameterGroup: Identifiable {
    let name: String
    var arguments: [TaskUIArgument]
    var id: String { name }
}

struct GenericTaskPanel: View {
    @ObservedObject var store: WorkbenchStore
    let tabID: String
    @State private var showingTaskList = true
    @State private var taskSearchText = ""
    @State private var categoryFilter: CasaTaskCategoryFilter = .all
    @State private var activeGenericSelectionHelper: String?
    @State private var genericChannelStartText = ""
    @State private var genericChannelEndText = ""
    @State private var genericChannelStepText = ""
    @State private var showingAdvancedParameters = false
    private let parameterGridColumns = [
        GridItem(.adaptive(minimum: 260), alignment: .topLeading)
    ]

    private var task: TaskCatalogEntry? {
        store.state.taskCatalog.first { $0.id == activeTaskID }
    }

    private var schema: TaskUISchema? {
        store.state.taskUISchemas[activeTaskID]
    }

    private var activeTaskID: String {
        store.taskID(forTab: tabID)
    }

    var body: some View {
        VStack(spacing: 0) {
            HStack {
                VStack(alignment: .leading, spacing: 2) {
                    Text(showingTaskList ? "Tasks" : (schema?.displayName ?? task?.displayName ?? "Task"))
                        .workbenchFont(.title3, weight: .semibold)
                    Text(showingTaskList ? "Choose a schema-backed CASA-rs task" : (schema?.summary ?? "Schema-backed CASA-rs task"))
                        .foregroundStyle(.secondary)
                }
                Spacer()
                if !showingTaskList {
                    Button {
                        showingTaskList = true
                    } label: {
                        Label("Change Task", systemImage: "list.bullet")
                    }
                    .accessibilityIdentifier("task.change")
                }
                Button {
                    store.selectTask(activeTaskID, tabID: tabID)
                    store.runTask()
                } label: {
                    Label(store.state.taskRun.state == .running ? "Running" : "Run", systemImage: "play.fill")
                }
                .disabled(
                    showingTaskList
                        || store.state.taskRun.state == .running
                        || schema == nil
                        || (store.taskRequiresConfirmation(taskID: activeTaskID) && !store.taskHasConfirmation(taskID: activeTaskID))
                )

                Button {
                    store.stopTask()
                } label: {
                    Label("Stop", systemImage: "stop.fill")
                }
                .disabled(store.state.taskRun.state != .running)
            }
            .padding()
            .background(.bar)

            ScrollView {
                VStack(alignment: .leading, spacing: 18) {
                    if prioritizesRunStatus {
                        runStatusBlock
                    }
                    if showingTaskList {
                        TaskCatalogBlock(
                            tasks: filteredTasks,
                            activeTaskID: activeTaskID,
                            categoryFilter: $categoryFilter,
                            searchText: $taskSearchText
                        ) { taskID in
                            store.selectTask(taskID, tabID: tabID)
                            showingTaskList = false
                        }
                    } else if let schema {
                        genericParameterBlock(schema: schema)
                        genericSavedJSONBlock
                        genericSafetyBlock
                    } else {
                        Text("Loading task schema...")
                            .foregroundStyle(.secondary)
                            .taskCard()
                    }
                    if !showingTaskList && !prioritizesRunStatus {
                        runStatusBlock
                    }
                }
                .padding(20)
            }
        }
        .task {
            if !activeTaskID.isEmpty {
                store.loadTaskUISchemaIfNeeded(activeTaskID)
            }
        }
        .onAppear {
            revealActiveTaskIfNeeded()
        }
        .onChange(of: activeTaskID) { _ in
            revealActiveTaskIfNeeded()
        }
    }

    private var activeTaskWasPreparedOutsideCatalog: Bool {
        guard !activeTaskID.isEmpty else {
            return false
        }
        if activeTaskID != "imager" {
            return true
        }
        if let values = store.state.genericTaskValues[activeTaskID], !values.isEmpty {
            return true
        }
        if let toggles = store.state.genericTaskToggles[activeTaskID], !toggles.isEmpty {
            return true
        }
        return false
    }

    private func revealActiveTaskIfNeeded() {
        guard activeTaskWasPreparedOutsideCatalog else { return }
        showingTaskList = false
    }

    private var filteredTasks: [TaskCatalogEntry] {
        let query = taskSearchText.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        return store.state.taskCatalog
            .filter { $0.showInSwift }
            .filter { !Self.explorerTaskIDs.contains($0.id) }
            .filter { categoryFilter.matches(task: $0) }
            .filter { task in
                query.isEmpty
                    || task.displayName.lowercased().contains(query)
                    || task.id.lowercased().contains(query)
                    || task.category.lowercased().contains(query)
                    || task.binaryName.lowercased().contains(query)
                    || task.shellKind.lowercased().contains(query)
            }
            .sorted {
                let ordering = $0.displayName.localizedCaseInsensitiveCompare($1.displayName)
                if ordering == .orderedSame {
                    return $0.id < $1.id
                }
                return ordering == .orderedAscending
            }
    }

    private static let explorerTaskIDs: Set<String> = ["imexplore", "msexplore", "tablebrowser"]

    private var prioritizesRunStatus: Bool {
        !showingTaskList && activeTaskID == "imager" && store.state.taskRun.imagerProgress != nil
    }

    private func genericParameterBlock(schema: TaskUISchema) -> some View {
        let groups = parameterGroups(for: schema)
        let hiddenAdvancedCount = schema.arguments
            .filter { !$0.hiddenInTUI && $0.advanced && !shouldRevealAdvancedArgument($0) }
            .count
        return VStack(alignment: .leading, spacing: 10) {
            HStack(alignment: .firstTextBaseline) {
                Text("Parameters")
                    .workbenchFont(.headline)
                Spacer()
                if hiddenAdvancedCount > 0 {
                    Toggle("Advanced", isOn: $showingAdvancedParameters)
                        .toggleStyle(.switch)
                        .controlSize(.small)
                        .help("Show less common parameters. CASA exposes many of these only after selecting related modes.")
                        .accessibilityIdentifier("task.parameters.advanced")
                }
            }
            ForEach(groups) { group in
                VStack(alignment: .leading, spacing: 6) {
                    if groups.count > 1 {
                        Text(group.name)
                            .workbenchFont(.caption, weight: .semibold)
                            .foregroundStyle(.secondary)
                    }
                    LazyVGrid(columns: parameterGridColumns, alignment: .leading, spacing: 8) {
                        ForEach(group.arguments) { argument in
                            genericControl(argument: argument)
                        }
                    }
                }
            }
        }
        .taskCard()
    }

    private func parameterGroups(for schema: TaskUISchema) -> [TaskParameterGroup] {
        let arguments = schema.arguments
            .filter { !$0.hiddenInTUI }
            .filter { shouldShowArgument($0) }
            .sorted { $0.order < $1.order }
        var groups: [TaskParameterGroup] = []
        for argument in arguments {
            let groupName = argument.group.trimmingCharacters(in: .whitespacesAndNewlines)
            let name = groupName.isEmpty ? "General" : groupName
            if let index = groups.firstIndex(where: { $0.name == name }) {
                groups[index].arguments.append(argument)
            } else {
                groups.append(TaskParameterGroup(name: name, arguments: [argument]))
            }
        }
        return groups
    }

    private func shouldShowArgument(_ argument: TaskUIArgument) -> Bool {
        if activeTaskID == "imager",
           ["niter", "threshold_jy"].contains(argument.id),
            genericTaskToggle("dirty_only") {
            return false
        }
        return !argument.advanced
            || showingAdvancedParameters
            || shouldRevealAdvancedArgument(argument)
    }

    private func shouldRevealAdvancedArgument(_ argument: TaskUIArgument) -> Bool {
        guard activeTaskID == "imager" else {
            return hasNonDefaultGenericValue(argument)
        }

        switch argument.id {
        case "robust":
            return ["briggs", "briggsbwtaper"].contains(genericTaskValue("weighting"))
        case "start", "width", "outframe", "veltype", "interpolation", "restfreq", "restoringbeam",
             "channel_start", "channel_count", "perchanweightdensity":
            return ["cube", "cubedata"].contains(genericTaskValue("specmode"))
        case "nterms":
            return genericTaskValue("deconvolver") == "mtmfs"
        case "scales", "smallscalebias":
            return genericTaskValue("deconvolver") == "multiscale"
        case "sidelobethreshold", "noisethreshold", "lownoisethreshold", "negativethreshold",
             "minbeamfrac", "growiterations":
            return genericTaskValue("usemask") == "auto-multithresh"
        case "wprojplanes":
            return genericTaskValue("gridder") == "wproject" || genericTaskValue("wterm") == "wproject"
        case "usepointing":
            return ["mosaic", "awproject", "awp2", "awphpg"].contains(genericTaskValue("gridder"))
        case "pblimit", "pbcor":
            return genericTaskToggle("write_pb") || genericTaskToggle("pbcor") || ["mosaic", "awproject", "awp2", "awphpg"].contains(genericTaskValue("gridder"))
        default:
            return hasNonDefaultGenericValue(argument)
        }
    }

    private func hasNonDefaultGenericValue(_ argument: TaskUIArgument) -> Bool {
        if argument.parser.kind == "toggle" {
            let current = genericTaskToggle(argument.id)
            let defaultValue = argument.default == "true"
            return current != defaultValue
        }
        let current = genericTaskValue(argument.id)
        let defaultValue = argument.default ?? ""
        return !current.isEmpty && current != defaultValue
    }

    private func genericTaskValue(_ argumentID: String) -> String {
        store.state.genericTaskValues[activeTaskID]?[argumentID]
            ?? schema?.arguments.first { $0.id == argumentID }?.default
            ?? ""
    }

    private func genericTaskToggle(_ argumentID: String) -> Bool {
        store.state.genericTaskToggles[activeTaskID]?[argumentID]
            ?? (schema?.arguments.first { $0.id == argumentID }?.default == "true")
    }

    @ViewBuilder
    private var genericSavedJSONBlock: some View {
        if activeTaskID == "simobserve", genericTaskValue("request_kind") == "family" {
            VStack(alignment: .leading, spacing: 10) {
                Text("Saved JSON")
                    .workbenchFont(.headline)
                HStack {
                    Button {
                        saveActiveGenericTaskRequest()
                    } label: {
                        Label("Save Request", systemImage: "square.and.arrow.down")
                    }
                    .disabled(!store.hasSaveableActiveGenericTaskRequest())
                    Button {
                        openGenericTaskRequest()
                    } label: {
                        Label("Open Request", systemImage: "folder")
                    }
                    Spacer()
                    Text(genericTaskValue("request_json"))
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                }
            }
            .taskCard()
        }
    }

    private func saveActiveGenericTaskRequest() {
        let panel = NSSavePanel()
        panel.allowedContentTypes = [.json]
        panel.canCreateDirectories = true
        panel.nameFieldStringValue = store.taskRequestSaveFilename()
        panel.directoryURL = URL(fileURLWithPath: store.taskRequestSaveDirectory(), isDirectory: true)
        guard panel.runModal() == .OK, let url = panel.url else {
            return
        }
        store.saveActiveGenericTaskRequest(to: url.path)
    }

    private func openGenericTaskRequest() {
        let panel = NSOpenPanel()
        panel.allowedContentTypes = [.json]
        panel.allowsMultipleSelection = false
        panel.canChooseDirectories = false
        panel.directoryURL = URL(fileURLWithPath: store.taskRequestSaveDirectory(), isDirectory: true)
        guard panel.runModal() == .OK, let url = panel.url else {
            return
        }
        store.loadGenericTaskRequest(from: url.path, tabID: tabID)
    }

    @ViewBuilder
    private var genericSafetyBlock: some View {
        if store.taskRequiresConfirmation(taskID: activeTaskID) {
            let taskID = activeTaskID
            let confirmed = Binding(
                get: { store.taskHasConfirmation(taskID: taskID) },
                set: { store.setGenericTaskConfirmation(taskID: taskID, confirmed: $0) }
            )
            VStack(alignment: .leading, spacing: 10) {
                Text("Safety")
                    .workbenchFont(.headline)
                Toggle("Confirm this task may modify data or create products", isOn: confirmed)
                if let row = store.taskExecutionMatrixRow(taskID: taskID) {
                    Text("\(row.mutationClass) / \(row.confirmation)")
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                }
            }
            .taskCard()
        }
    }

    @ViewBuilder
    private func genericControl(argument: TaskUIArgument) -> some View {
        let taskID = activeTaskID
        let value = Binding(
            get: { store.state.genericTaskValues[taskID]?[argument.id] ?? argument.default ?? "" },
            set: { store.setGenericTaskValue(taskID: taskID, argumentID: argument.id, value: $0) }
        )
        let toggle = Binding(
            get: { store.state.genericTaskToggles[taskID]?[argument.id] ?? (argument.default == "true") },
            set: { store.setGenericTaskToggle(taskID: taskID, argumentID: argument.id, value: $0) }
        )
        let label = displayLabel(for: argument)
        let selectableChoices = choices(for: argument)

        if argument.parser.kind == "toggle" {
            Toggle(label, isOn: toggle)
                .help(argument.help)
                .frame(maxWidth: .infinity, alignment: .leading)
        } else {
            VStack(alignment: .leading, spacing: 3) {
                Text(label)
                    .workbenchFont(.caption, weight: .semibold)
                    .foregroundStyle(.secondary)
                if isRegionArgument(argument) {
                    DatasetPathInputControl(
                        store: store,
                        label: label,
                        value: value,
                        help: argument.help,
                        choices: datasetPathChoices(for: argument),
                        browseParameterType: argument.parameterType,
                        canBrowse: true,
                        actionTitle: nil,
                        actionSystemImage: "folder",
                        allowInlineSyntax: true,
                        syncSelectedRegion: false,
                        onAction: nil
                    )
                } else if isPathArgument(argument) {
                    DatasetPathInputControl(
                        store: store,
                        label: label,
                        value: value,
                        help: argument.help,
                        choices: datasetPathChoices(for: argument),
                        browseParameterType: argument.parameterType,
                        canBrowse: canBrowse(argument: argument),
                        actionTitle: nil,
                        actionSystemImage: "folder",
                        allowInlineSyntax: false,
                        syncSelectedRegion: false,
                        onAction: nil
                    )
                } else if !selectableChoices.isEmpty {
                    Picker(label, selection: value) {
                        ForEach(selectableChoices, id: \.self) { choice in
                            Text(choice).tag(choice)
                        }
                    }
                    .labelsHidden()
                    .help(argument.help)
                } else if isChannelSelectionArgument(argument) {
                    genericChannelSelectionControl(label: label, value: value, argument: argument)
                } else {
                    TextField(prompt(for: argument), text: value)
                        .textFieldStyle(.roundedBorder)
                        .help(helpText(for: argument))
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
        }
    }

    private func genericChannelSelectionControl(
        label: String,
        value: Binding<String>,
        argument: TaskUIArgument
    ) -> some View {
        let limit = genericImageChannelLimit()
        let isValid = isValidGenericChannelSelection(value.wrappedValue, channelLimit: limit)
        return VStack(alignment: .leading, spacing: 4) {
            HStack(spacing: 6) {
                TextField(label, text: value)
                    .textFieldStyle(.roundedBorder)
                    .foregroundStyle(isValid ? Color.primary : Color.red)
                    .help(helpText(for: argument))
                Button {
                    populateGenericChannelFieldsForHelper(from: value.wrappedValue)
                    activeGenericSelectionHelper = argument.id
                } label: {
                    Image(systemName: "number")
                }
                .buttonStyle(.borderless)
                .help("Build a CASA channel selector from the selected image channel range.")
                .popover(isPresented: Binding(
                    get: { activeGenericSelectionHelper == argument.id },
                    set: { isPresented in
                        if !isPresented && activeGenericSelectionHelper == argument.id {
                            activeGenericSelectionHelper = nil
                        }
                    }
                )) {
                    genericChannelHelperPopover(text: value)
                }
            }
            if let limit {
                Text("Valid channels: 0 through \(max(0, limit - 1))")
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                Text(genericChannelSelectionSummary(value.wrappedValue, channelLimit: limit))
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
            }
        }
    }

    private func genericChannelHelperPopover(text: Binding<String>) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Builds CASA channel syntax such as 4~12 or 0~14^2 from the selected image shape. Leave blank for all channels.")
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
            if let limit = genericImageChannelLimit() {
                Text("Valid channel IDs: 0 through \(max(0, limit - 1))")
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                Text(genericChannelSelectionSummary(text.wrappedValue, channelLimit: limit))
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
            } else {
                Text("Select an image with a known spectral axis to validate the range.")
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
            }
            HStack {
                TextField("start", text: $genericChannelStartText)
                    .textFieldStyle(.roundedBorder)
                Text("to")
                    .foregroundStyle(.secondary)
                TextField("end", text: $genericChannelEndText)
                    .textFieldStyle(.roundedBorder)
                Text("step")
                    .foregroundStyle(.secondary)
                TextField("1", text: $genericChannelStepText)
                    .textFieldStyle(.roundedBorder)
                    .frame(width: 48)
            }
            HStack {
                Button("Apply range") {
                    applyGenericChannelSelection(text: text)
                }
                Button("Clear") {
                    genericChannelStartText = ""
                    genericChannelEndText = ""
                    genericChannelStepText = ""
                    text.wrappedValue = ""
                }
                Spacer()
                Button("All") {
                    text.wrappedValue = ""
                    populateGenericChannelFieldsForHelper(from: "")
                }
            }
            Divider()
            ScrollView {
                VStack(alignment: .leading, spacing: 6) {
                    ForEach(genericChannelSelectionOptions) { option in
                        Button(option.label) {
                            text.wrappedValue = option.value
                            populateGenericChannelFieldsForHelper(from: option.value)
                        }
                    }
                }
                .frame(maxWidth: .infinity, alignment: .leading)
            }
            .frame(maxHeight: 150)
        }
        .padding(12)
        .frame(width: 420, alignment: .leading)
        .onAppear {
            populateGenericChannelFieldsForHelper(from: text.wrappedValue)
        }
    }

    private var genericChannelSelectionOptions: [SelectionHelperOption] {
        guard let limit = genericImageChannelLimit(), limit > 0 else {
            return [SelectionHelperOption(label: "All channels", value: "")]
        }
        var options = [SelectionHelperOption(label: "All channels", value: "")]
        options.append(SelectionHelperOption(label: "First channel", value: "0"))
        if limit > 1 {
            options.append(SelectionHelperOption(label: "All explicit", value: "0~\(limit - 1)"))
        }
        if limit >= 8 {
            let start = limit / 4
            let end = max(start, (limit * 3 / 4) - 1)
            options.append(SelectionHelperOption(label: "Middle half", value: "\(start)~\(end)"))
        }
        if limit >= 16 {
            options.append(SelectionHelperOption(label: "Every fourth", value: "0~\(limit - 1)^4"))
        }
        return options
    }

    private func applyGenericChannelSelection(text: Binding<String>) {
        let start = genericChannelStartText.trimmingCharacters(in: .whitespacesAndNewlines)
        let end = genericChannelEndText.trimmingCharacters(in: .whitespacesAndNewlines)
        let step = genericChannelStepText.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !start.isEmpty, Int(start) != nil else {
            return
        }
        let candidate: String
        if end.isEmpty {
            candidate = start
        } else if step.isEmpty || step == "1" {
            candidate = "\(start)~\(end)"
        } else {
            candidate = "\(start)~\(end)^\(step)"
        }
        if isValidGenericChannelSelection(candidate, channelLimit: genericImageChannelLimit()) {
            text.wrappedValue = candidate
        }
    }

    private func populateGenericChannelFields(from value: String) {
        let first = value
            .split(whereSeparator: { $0 == ";" || $0 == "," })
            .first
            .map(String.init) ?? ""
        guard !first.isEmpty else {
            genericChannelStartText = ""
            genericChannelEndText = ""
            genericChannelStepText = ""
            return
        }
        let stepped = first.split(separator: "^", omittingEmptySubsequences: false)
        genericChannelStepText = stepped.count == 2 ? String(stepped[1]) : ""
        let range = stepped[0].split(separator: "~", omittingEmptySubsequences: false)
        genericChannelStartText = range.first.map(String.init) ?? ""
        genericChannelEndText = range.count == 2 ? String(range[1]) : ""
    }

    private func populateGenericChannelFieldsForHelper(from value: String) {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.isEmpty else {
            populateGenericChannelFields(from: value)
            return
        }
        genericChannelStartText = "0"
        genericChannelEndText = genericImageChannelLimit().map { String(max(0, $0 - 1)) } ?? ""
        genericChannelStepText = ""
    }

    private func genericChannelSelectionSummary(_ value: String, channelLimit: Int) -> String {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        if trimmed.isEmpty {
            return "Current selector: all channels (0~\(max(0, channelLimit - 1)))"
        }
        return "Current selector: \(trimmed)"
    }

    private func isValidGenericChannelSelection(_ value: String, channelLimit: Int?) -> Bool {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty, let channelLimit, channelLimit > 0 else {
            return true
        }
        return trimmed
            .split(whereSeparator: { $0 == ";" || $0 == "," })
            .allSatisfy { rawPart in
                let part = String(rawPart).trimmingCharacters(in: .whitespacesAndNewlines)
                guard !part.isEmpty else { return false }
                let stepped = part.split(separator: "^", omittingEmptySubsequences: false)
                guard stepped.count <= 2 else { return false }
                if stepped.count == 2 {
                    guard let step = Int(stepped[1]), step > 0 else { return false }
                }
                let rangeParts = stepped[0].split(separator: "~", omittingEmptySubsequences: false)
                if rangeParts.count == 1, let channel = Int(rangeParts[0]) {
                    return channel >= 0 && channel < channelLimit
                }
                guard rangeParts.count == 2,
                      let start = Int(rangeParts[0]),
                      let end = Int(rangeParts[1])
                else {
                    return false
                }
                return start >= 0 && start <= end && end < channelLimit
            }
    }

    private func isChannelSelectionArgument(_ argument: TaskUIArgument) -> Bool {
        argument.id == "chans" || argument.parameterType == "channel_selector"
    }

    private func prompt(for argument: TaskUIArgument) -> String {
        switch argument.id {
        case "chans":
            return "4~12 or 0~14^2"
        case "includepix":
            return "0.03,100"
        default:
            if let metavar = argument.parser.metavar?.trimmingCharacters(in: .whitespacesAndNewlines),
               !metavar.isEmpty {
                return metavar
            }
            return displayLabel(for: argument)
        }
    }

    private func helpText(for argument: TaskUIArgument) -> String {
        let base = argument.help.trimmingCharacters(in: .whitespacesAndNewlines)
        let example: String?
        switch argument.id {
        case "chans":
            example = "Accepted channel syntax: single channel 4, inclusive range 4~12, or stepped range 0~14^2. Leave blank for all channels."
        case "includepix":
            example = "Accepted pixel range syntax: min,max, for example 0.03,100. Pixels outside the inclusive range are excluded."
        default:
            example = nil
        }
        guard let example else {
            return base
        }
        return base.isEmpty ? example : "\(base) \(example)"
    }

    private func genericImageChannelLimit() -> Int? {
        guard let dataset = genericImageDataset(), dataset.shape.count >= 3 else {
            return nil
        }
        let nonSpatialAxes = dataset.shape.dropFirst(2)
        if let spectralLikeAxis = nonSpatialAxes.reversed().first(where: { $0 > 1 }) {
            return Int(spectralLikeAxis)
        }
        return 1
    }

    private func genericImageDataset() -> DatasetSummary? {
        guard let schema else {
            return store.state.selectedDataset?.kind == .imageCube ? store.state.selectedDataset : nil
        }
        let values = store.state.genericTaskValues[schema.commandID] ?? [:]
        let imageArgument = schema.arguments.first { argument in
            argument.id == "imagename"
                || argument.id == "image_path"
                || argument.parameterType == "image_path"
        }
        if let argument = imageArgument,
           let value = values[argument.id],
           let dataset = imageDataset(matching: value) {
            return dataset
        }
        return store.state.selectedDataset?.kind == .imageCube ? store.state.selectedDataset : nil
    }

    private func imageDataset(matching value: String) -> DatasetSummary? {
        let absolute = absolutePath(fromDisplayedPath: value)
        let standardized = URL(fileURLWithPath: absolute).standardizedFileURL.path
        return store.state.project.datasets.first { dataset in
            guard dataset.kind == .imageCube else { return false }
            let datasetPath = URL(fileURLWithPath: dataset.path).standardizedFileURL.path
            return datasetPath == standardized || dataset.name == value
        }
    }

    private func choices(for argument: TaskUIArgument) -> [String] {
        if argument.parameterType == "measurement_set_path" || ["ms", "vis"].contains(argument.id) {
            let measurementSets = store.state.project.datasets
                .filter { $0.kind == .measurementSet }
                .map(\.path)
            if !measurementSets.isEmpty {
                return measurementSets
            }
        }
        if argument.parameterType == "image_path" {
            let images = store.state.project.datasets
                .filter { $0.kind == .imageCube }
                .map(\.path)
            if !images.isEmpty {
                return images
            }
        }
        if argument.parameterType == "fits_path" {
            let fits = store.state.project.datasets
                .filter(isFitsDataset)
                .map(\.path)
            if !fits.isEmpty {
                return fits
            }
        }
        if argument.parameterType == "region_path_or_box" || argument.id == "region" {
            let regions = store.state.project.datasets
                .filter { $0.kind == .region }
                .map(\.path)
            if !regions.isEmpty {
                return regions
            }
        }
        if ["table_path", "calibration_table_path"].contains(argument.parameterType ?? "") {
            let tables = store.state.project.datasets
                .filter { dataset in
                    dataset.kind == .table || dataset.kind == .calibrationTable
                }
                .map(\.path)
            if !tables.isEmpty {
                return tables
            }
        }
        if argument.parameterType == "spectral_window_selector" || argument.id == "spw",
           let spectralWindows = store.state.selectedDataset?.spectralWindows,
           !spectralWindows.isEmpty {
            return spectralWindows.compactMap { label in
                label.split(separator: ":", maxSplits: 1).first?.split(separator: " ").last.map(String.init)
            }
        }
        if ["field_selector", "field_id"].contains(argument.parameterType ?? "") || ["field", "phasecenter_field"].contains(argument.id),
           let fields = store.state.selectedDataset?.fields,
           !fields.isEmpty {
            return fields.compactMap { label in
                label.split(separator: ":", maxSplits: 1).first?.split(separator: " ").last.map(String.init)
            }
        }
        if argument.parameterType == "scan_selector" || argument.id == "scan",
           let scans = store.state.selectedDataset?.scans,
           !scans.isEmpty {
            return scans.compactMap { label in
                label.split(separator: ":", maxSplits: 1).first?.split(separator: " ").last.map(String.init)
            }
        }
        if argument.parameterType == "antenna_selector" || argument.id == "antenna",
           let antennas = store.state.selectedDataset?.antennas,
           !antennas.isEmpty {
            return antennas
        }
        if argument.parameterType == "correlation_or_stokes" || argument.id == "correlation",
           let correlations = store.state.selectedDataset?.correlations,
           !correlations.isEmpty {
            return correlations
        }
        if argument.parameterType == "data_column" || argument.id == "datacolumn",
           let columns = store.state.selectedDataset?.dataColumns,
           !columns.isEmpty {
            return columns
        }
        return argument.parser.choices ?? []
    }

    private func datasetPathChoices(for argument: TaskUIArgument) -> [DatasetPathChoice] {
        choices(for: argument).map { path in
            DatasetPathChoice(name: displayPath(path), path: storedPathValue(fromDisplayedPath: path))
        }
    }

    private func isFitsDataset(_ dataset: DatasetSummary) -> Bool {
        guard dataset.kind == .runProduct else {
            return false
        }
        switch URL(fileURLWithPath: dataset.path).pathExtension.lowercased() {
        case "fits", "fit", "fts":
            return true
        default:
            return false
        }
    }

    private func displayLabel(for argument: TaskUIArgument) -> String {
        let label = argument.label.trimmingCharacters(in: .whitespacesAndNewlines)
        if !label.isEmpty {
            return label
        }
        return argument.id
            .split(separator: "_")
            .map { part in
                part.prefix(1).uppercased() + String(part.dropFirst())
            }
            .joined(separator: " ")
    }

    private func isPathArgument(_ argument: TaskUIArgument) -> Bool {
        argument.valueKind == "path" || argument.parameterType?.contains("path") == true
    }

    private func isRegionArgument(_ argument: TaskUIArgument) -> Bool {
        argument.parameterType == "region_path_or_box" || argument.id == "region"
    }

    private func canBrowse(argument: TaskUIArgument) -> Bool {
        guard isPathArgument(argument) else {
            return false
        }
        return true
    }

    private func taskPathChooserRoot() -> URL? {
        let root = store.state.project.rootPath.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !root.isEmpty else {
            return nil
        }
        return URL(fileURLWithPath: (root as NSString).expandingTildeInPath, isDirectory: true)
            .standardizedFileURL
    }

    private func pathValueBinding(rawValue: Binding<String>) -> Binding<String> {
        Binding(
            get: { displayPath(rawValue.wrappedValue) },
            set: { rawValue.wrappedValue = storedPathValue(fromDisplayedPath: $0) }
        )
    }

    private func pathDisplayBinding(rawValue: Binding<String>, argument: TaskUIArgument) -> Binding<String> {
        guard argument.parameterType == "region_path_or_box" else {
            return pathValueBinding(rawValue: rawValue)
        }
        return Binding(
            get: {
                isInlineRegionSyntax(rawValue.wrappedValue) ? rawValue.wrappedValue : displayPath(rawValue.wrappedValue)
            },
            set: { value in
                rawValue.wrappedValue = isInlineRegionSyntax(value) ? value : storedPathValue(fromDisplayedPath: value)
            }
        )
    }

    private func isInlineRegionSyntax(_ value: String) -> Bool {
        let trimmed = value.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        return trimmed.hasPrefix("box[[")
            || trimmed.hasPrefix("poly [[")
            || trimmed.hasPrefix("box:")
            || trimmed.hasPrefix("pixelbox(")
    }

    private func displayPath(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return path
        }
        let root = store.state.project.rootPath
        guard !root.isEmpty else {
            return path
        }
        let rootURL = URL(fileURLWithPath: root, isDirectory: true).standardizedFileURL
        let pathURL = URL(fileURLWithPath: (trimmed as NSString).expandingTildeInPath).standardizedFileURL
        let rootPath = rootURL.path
        let absolutePath = pathURL.path
        if absolutePath == rootPath {
            return "."
        }
        let prefix = rootPath.hasSuffix("/") ? rootPath : rootPath + "/"
        if absolutePath.hasPrefix(prefix) {
            return String(absolutePath.dropFirst(prefix.count))
        }
        return path
    }

    private func storedPathValue(fromDisplayedPath path: String) -> String {
        let trimmed = path.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return path
        }
        let expanded = (trimmed as NSString).expandingTildeInPath
        if expanded.hasPrefix("/") {
            return displayPath(expanded)
        }
        return trimmed
    }

    private func absolutePath(fromDisplayedPath path: String) -> String {
        let trimmed = path.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return path
        }
        let expanded = (trimmed as NSString).expandingTildeInPath
        if expanded.hasPrefix("/") {
            return expanded
        }
        let root = store.state.project.rootPath
        guard !root.isEmpty else {
            return path
        }
        return URL(fileURLWithPath: root, isDirectory: true)
            .appendingPathComponent(trimmed)
            .standardizedFileURL
            .path
    }

    private var runStatusBlock: some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Run")
                .workbenchFont(.headline)
            if activeTaskID == "imager", let progress = store.state.taskRun.imagerProgress {
                WorkEstimateBar(estimate: progress.workEstimate)
            } else {
                RunProgressBar(progress: store.state.taskRun.progress)
            }
            Text(store.state.taskRun.state.rawValue)
                .foregroundStyle(.secondary)
            if activeTaskID == "imager", let progress = store.state.taskRun.imagerProgress {
                ImagerProgressDashboard(snapshot: progress)
            }
            valueList("Log", values: store.state.taskRun.logLines)
            valueList("Diagnostics", values: store.state.taskRun.diagnostics)
            valueList("Products", values: store.state.taskRun.products.map(displayPath))
            Button {
                saveTaskOutput()
            } label: {
                Label("Save Output...", systemImage: "square.and.arrow.down")
            }
            .disabled(!store.hasSaveableActiveTaskOutput())
            .help("Save the latest task output to a file")
        }
        .taskCard()
    }

    private func saveTaskOutput() {
        let panel = NSSavePanel()
        panel.title = "Save Task Output"
        panel.prompt = "Save"
        panel.directoryURL = URL(fileURLWithPath: store.taskOutputSaveDirectory(), isDirectory: true)
        panel.nameFieldStringValue = store.taskOutputSaveFilename()
        panel.canCreateDirectories = true
        panel.allowedContentTypes = store.taskOutputSaveFilename().hasSuffix(".json") ? [.json] : [.plainText]
        if panel.runModal() == .OK, let url = panel.url {
            store.saveActiveTaskOutput(to: url.path)
        }
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

private struct DatasetPathChoice: Hashable {
    let name: String
    let path: String
}

private struct DatasetPathInputControl: View {
    @ObservedObject var store: WorkbenchStore
    let label: String
    @Binding var value: String
    let help: String
    let choices: [DatasetPathChoice]
    var browseParameterType: String? = nil
    let canBrowse: Bool
    let actionTitle: String?
    let actionSystemImage: String
    let allowInlineSyntax: Bool
    let syncSelectedRegion: Bool
    let onAction: ((String) -> Void)?

    var body: some View {
        HStack(spacing: 6) {
            TextField(label, text: displayBinding)
                .textFieldStyle(.roundedBorder)
                .help(helpText)

            if !choices.isEmpty {
                Menu {
                    ForEach(choices, id: \.self) { choice in
                        Button(choice.name) {
                            value = storedPathValue(fromDisplayedPath: choice.path)
                        }
                        .help(displayPath(choice.path))
                    }
                } label: {
                    Image(systemName: "list.bullet.rectangle")
                }
                .buttonStyle(.borderless)
                .help("Use a path from the dataset list")
            }

            if canBrowse {
                Button {
                    if let url = TaskParameterOpenPanel.choosePath(
                        parameterType: browseParameterType ?? "region_path_or_box",
                        directoryURL: projectRootURL()
                    ) {
                        value = storedPathValue(fromDisplayedPath: url.path)
                    }
                } label: {
                    Image(systemName: "folder")
                }
                .buttonStyle(.borderless)
                .help("Choose \(label)")
            }

            if let actionTitle, let onAction {
                Button {
                    onAction(resolvedFilePath)
                } label: {
                    Label(actionTitle, systemImage: actionSystemImage)
                }
                .disabled(resolvedFilePath.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty || isInlineRegionSyntax(value))
                .help("Load the selected region file")
            }
        }
        .onAppear(perform: syncSelectedRegionIfNeeded)
        .onChange(of: store.state.selectedDataset?.id) { _ in
            syncSelectedRegionIfNeeded()
        }
    }

    private var displayBinding: Binding<String> {
        Binding(
            get: {
                if allowInlineSyntax, isInlineRegionSyntax(value) {
                    return value
                }
                return displayPath(value)
            },
            set: { nextValue in
                if allowInlineSyntax, isInlineRegionSyntax(nextValue) {
                    value = nextValue
                } else {
                    value = storedPathValue(fromDisplayedPath: nextValue)
                }
            }
        )
    }

    private var resolvedFilePath: String {
        if allowInlineSyntax, isInlineRegionSyntax(value) {
            return value
        }
        return absolutePath(fromDisplayedPath: value)
    }

    private var helpText: String {
        if allowInlineSyntax {
            return help
        }
        return "\(help) Inline CRTF syntax is accepted by analysis tasks, but Image Explorer loading requires a CRTF file."
    }

    private func syncSelectedRegionIfNeeded() {
        guard syncSelectedRegion, let selected = store.state.selectedDataset, selected.kind == .region else {
            return
        }
        value = storedPathValue(fromDisplayedPath: selected.path)
    }

    private func projectRootURL() -> URL? {
        let root = store.state.project.rootPath.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !root.isEmpty else {
            return nil
        }
        return URL(fileURLWithPath: (root as NSString).expandingTildeInPath, isDirectory: true)
            .standardizedFileURL
    }

    private func displayPath(_ path: String) -> String {
        let trimmed = path.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return path
        }
        guard let rootURL = projectRootURL() else {
            return path
        }
        let pathURL = URL(fileURLWithPath: (trimmed as NSString).expandingTildeInPath).standardizedFileURL
        let rootPath = rootURL.path
        let absolutePath = pathURL.path
        if absolutePath == rootPath {
            return "."
        }
        let prefix = rootPath.hasSuffix("/") ? rootPath : rootPath + "/"
        if absolutePath.hasPrefix(prefix) {
            return String(absolutePath.dropFirst(prefix.count))
        }
        return path
    }

    private func storedPathValue(fromDisplayedPath path: String) -> String {
        let trimmed = path.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return path
        }
        let expanded = (trimmed as NSString).expandingTildeInPath
        if expanded.hasPrefix("/") {
            return displayPath(expanded)
        }
        return trimmed
    }

    private func absolutePath(fromDisplayedPath path: String) -> String {
        let trimmed = path.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !trimmed.isEmpty else {
            return path
        }
        let expanded = (trimmed as NSString).expandingTildeInPath
        if expanded.hasPrefix("/") {
            return expanded
        }
        guard let rootURL = projectRootURL() else {
            return path
        }
        return rootURL.appendingPathComponent(trimmed).standardizedFileURL.path
    }

    private func isInlineRegionSyntax(_ text: String) -> Bool {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
        return trimmed.hasPrefix("box[[")
            || trimmed.hasPrefix("poly [[")
            || trimmed.hasPrefix("box:")
            || trimmed.hasPrefix("pixelbox(")
    }
}

private enum TaskParameterOpenPanel {
    static func choosePath(parameterType: String?, directoryURL: URL?) -> URL? {
        if isOutputPath(parameterType: parameterType) {
            let panel = NSSavePanel()
            panel.prompt = "Choose"
            panel.message = message(for: parameterType)
            panel.directoryURL = directoryURL
            panel.canCreateDirectories = true
            if isFitsPath(parameterType: parameterType) {
                panel.allowedContentTypes = ["fit", "fits", "fts"].compactMap { UTType(filenameExtension: $0) }
            }
            guard panel.runModal() == .OK, let url = panel.url else {
                return nil
            }
            return url
        }
        let panel = NSOpenPanel()
        panel.allowsMultipleSelection = false
        panel.prompt = "Choose"
        panel.message = message(for: parameterType)
        panel.directoryURL = directoryURL
        panel.canChooseDirectories = acceptsDirectories(parameterType: parameterType)
        panel.canChooseFiles = acceptsFiles(parameterType: parameterType)
        if isFitsPath(parameterType: parameterType) {
            panel.allowedContentTypes = ["fit", "fits", "fts"].compactMap { UTType(filenameExtension: $0) }
        }
        guard panel.runModal() == .OK, let url = panel.url else {
            return nil
        }
        guard selectedPathIsAllowed(url, parameterType: parameterType) else {
            NSSound.beep()
            return nil
        }
        return url
    }

    private static func message(for parameterType: String?) -> String {
        switch parameterType {
        case "measurement_set_path":
            return "Choose a MeasurementSet directory ending in .ms."
        case "image_path":
            return "Choose a CASA image directory."
        case "calibration_table_path":
            return "Choose a CASA calibration table directory."
        case "table_path":
            return "Choose a CASA table directory."
        case "fits_path":
            return "Choose a FITS file."
        case "output_fits_path":
            return "Choose where to write the FITS file."
        case "output_image_path":
            return "Choose where to write the CASA image directory."
        case "output_measurement_set_path":
            return "Choose where to write the MeasurementSet directory."
        case "output_calibration_table_path":
            return "Choose where to write the calibration table directory."
        case "region_path_or_box":
            return "Choose a CASA CRTF region file, or type inline CRTF such as box[[100pix,100pix],[150pix,150pix]] or poly [[100pix,100pix],...]."
        default:
            return "Choose a path."
        }
    }

    private static func acceptsDirectories(parameterType: String?) -> Bool {
        switch parameterType {
        case "fits_path", "output_fits_path":
            return false
        default:
            return true
        }
    }

    private static func acceptsFiles(parameterType: String?) -> Bool {
        switch parameterType {
        case "measurement_set_path", "image_path", "calibration_table_path", "table_path":
            return false
        default:
            return true
        }
    }

    private static func selectedPathIsAllowed(_ url: URL, parameterType: String?) -> Bool {
        switch parameterType {
        case "measurement_set_path":
            return isDirectory(url) && url.pathExtension.localizedCaseInsensitiveCompare("ms") == .orderedSame
        case "fits_path", "output_fits_path":
            return ["fit", "fits", "fts"].contains(url.pathExtension.lowercased())
        case "image_path", "calibration_table_path", "table_path":
            return isDirectory(url)
        default:
            return true
        }
    }

    private static func isDirectory(_ url: URL) -> Bool {
        var isDirectory = ObjCBool(false)
        return FileManager.default.fileExists(atPath: url.path, isDirectory: &isDirectory) && isDirectory.boolValue
    }

    private static func isOutputPath(parameterType: String?) -> Bool {
        parameterType?.hasPrefix("output_") == true
    }

    private static func isFitsPath(parameterType: String?) -> Bool {
        parameterType == "fits_path" || parameterType == "output_fits_path"
    }
}

struct TaskCatalogBlock: View {
    var tasks: [TaskCatalogEntry]
    var activeTaskID: String
    @Binding var categoryFilter: CasaTaskCategoryFilter
    @Binding var searchText: String
    var selectTask: ((String) -> Void)? = nil
    private static let explorerTaskIDs: Set<String> = ["imexplore", "msexplore", "tablebrowser"]

    private var taskRows: [TaskCatalogEntry] {
        tasks.filter { !Self.explorerTaskIDs.contains($0.id) }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Picker("Category", selection: $categoryFilter) {
                ForEach(CasaTaskCategoryFilter.allCases) { category in
                    Text(category.title).tag(category)
                }
            }
            .accessibilityIdentifier("task.categoryFilter")

            TextField("Search tasks", text: $searchText)
                .textFieldStyle(.roundedBorder)
                .help("Filter tasks by name, id, category, or summary.")
                .accessibilityIdentifier("task.search")

            if taskRows.isEmpty {
                Text("No matching tasks")
                    .foregroundStyle(.secondary)
            }

            ForEach(taskRows) { task in
                Button {
                    selectTask?(task.id)
                } label: {
                    HStack {
                        taskRow(task)
                    }
                }
                .buttonStyle(.plain)
                .disabled(selectTask == nil)
            }
        }
        .taskCard()
        .accessibilityIdentifier("task.catalog")
    }

    private func taskRow(_ task: TaskCatalogEntry) -> some View {
        HStack(alignment: .top, spacing: 10) {
            Image(systemName: CasaTaskCategoryFilter.categoryIcon(for: task))
                .foregroundStyle(task.id == activeTaskID ? Color.accentColor : Color.secondary)
                .frame(width: 18, height: 18)
                .padding(.top, 1)

            VStack(alignment: .leading, spacing: 2) {
                Text(task.displayName)
                    .fontWeight(task.id == activeTaskID ? .semibold : .regular)
                Text("\(CasaTaskCategoryFilter.categoryTitle(for: task)) / \(task.shellKind)")
                    .foregroundStyle(.secondary)
            }
            Spacer()
        }
    }
}

enum CasaTaskCategoryFilter: String, CaseIterable, Identifiable {
    case all
    case inputOutput
    case information
    case flagging
    case calibration
    case imaging
    case singleDish
    case manipulation
    case analysis
    case visualization
    case simulation

    var id: String { rawValue }

    var title: String {
        switch self {
        case .all: return "All Tasks"
        case .inputOutput: return "Input / Output"
        case .information: return "Information"
        case .flagging: return "Flagging"
        case .calibration: return "Calibration"
        case .imaging: return "Imaging"
        case .singleDish: return "Single Dish"
        case .manipulation: return "Manipulation"
        case .analysis: return "Analysis"
        case .visualization: return "Visualization"
        case .simulation: return "Simulation"
        }
    }

    func matches(task: TaskCatalogEntry) -> Bool {
        self == .all || Self.category(for: task) == self
    }

    static func categoryTitle(for task: TaskCatalogEntry) -> String {
        category(for: task).title
    }

    static func categoryIcon(for task: TaskCatalogEntry) -> String {
        switch category(for: task) {
        case .all:
            return "square.grid.2x2"
        case .inputOutput:
            return "arrow.left.arrow.right"
        case .information:
            return "info.circle"
        case .flagging:
            return "flag"
        case .calibration:
            return "slider.horizontal.3"
        case .imaging:
            return "photo"
        case .singleDish:
            return "dot.radiowaves.left.and.right"
        case .manipulation:
            return "rectangle.2.swap"
        case .analysis:
            return "chart.xyaxis.line"
        case .visualization:
            return "eye"
        case .simulation:
            return "waveform.path.ecg"
        }
    }

    static func category(for task: TaskCatalogEntry) -> CasaTaskCategoryFilter {
        switch task.id {
        case "importvla", "importfits", "exportfits":
            return .inputOutput
        case "msexplore", "tablebrowser":
            return .information
        case "flagdata", "flagmanager":
            return .flagging
        case "calibrate", "applycal", "bandpass", "fluxscale", "gaincal", "gencal":
            return .calibration
        case "imager", "feather":
            return .imaging
        case "mstransform", "split", "uvcontsub":
            return .manipulation
        case "immath", "immoments", "impv", "imregrid", "imsubimage":
            return .analysis
        case "imexplore":
            return .visualization
        case "simobserve":
            return .simulation
        default:
            switch task.category.lowercased() {
            case "import":
                return .inputOutput
            case "measurementset":
                return .information
            case "flagging":
                return .flagging
            case "calibration":
                return .calibration
            case "imaging":
                return .imaging
            case "images":
                return .analysis
            case "simulation":
                return .simulation
            default:
                return .information
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
