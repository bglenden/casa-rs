import CasarsMacCore
import AppKit
import OSLog
import SwiftUI

private let inspectorLogger = Logger(
    subsystem: "org.casa-rs.casars-mac",
    category: "Inspector"
)

private let datasetClickLogger = Logger(
    subsystem: "org.casa-rs.casars-mac",
    category: "DatasetClick"
)

struct WorkbenchView: View {
    @ObservedObject var store: WorkbenchStore
    var initialMeasurementSetExplorerMode: MeasurementSetExplorerMode = .summary
    @State private var leftDockWidth: CGFloat = 250
    @State private var inspectorWidth: CGFloat = 250
    @State private var aiDrawerWidth: CGFloat = 400

    var body: some View {
        HStack(spacing: 0) {
            if !store.state.leftDockCollapsed {
                LeftDockView(store: store)
                    .frame(width: leftDockWidth)

                HorizontalResizeHandle(
                    width: $leftDockWidth,
                    range: 190...420,
                    anchor: .left,
                    accessibilityID: "split.resizeHandle"
                )
            }

            if !store.state.inspectorCollapsed {
                InspectorView(store: store)
                    .frame(width: inspectorWidth)

                HorizontalResizeHandle(
                    width: $inspectorWidth,
                    range: 220...520,
                    anchor: .left,
                    accessibilityID: "split.resizeHandle"
                )
            }

            CentralWorkspaceView(
                store: store,
                initialMeasurementSetExplorerMode: initialMeasurementSetExplorerMode
            )
                .frame(minWidth: 560)

            if store.isAIPrototypeRuntime,
               store.state.prototypeAI?.presentation == .drawer
            {
                HorizontalResizeHandle(
                    width: $aiDrawerWidth,
                    range: 340...520,
                    anchor: .right,
                    accessibilityID: "aiPrototype.resizeHandle"
                )

                AIChatPrototypeView(store: store, layout: .drawer)
                    .frame(width: aiDrawerWidth)
                    .transition(.move(edge: .trailing).combined(with: .opacity))
            }
        }
        .animation(.easeInOut(duration: 0.18), value: store.state.prototypeAI?.presentation)
        .toolbar {
            ToolbarItem(placement: .navigation) {
                Button {
                    store.toggleLeftDock()
                } label: {
                    Image(systemName: store.state.leftDockCollapsed ? "sidebar.left" : "sidebar.leading")
                }
                .help(store.state.leftDockCollapsed ? "Show Left Dock" : "Hide Left Dock")
                .accessibilityLabel(store.state.leftDockCollapsed ? "Show Left Dock" : "Hide Left Dock")
                .accessibilityIdentifier(store.state.leftDockCollapsed ? "dock.restore" : "dock.collapse")
            }

            ToolbarItem(placement: .principal) {
                CommandSearchField(store: store)
            }

        }
        .onReceive(Timer.publish(every: 1.0, on: .main, in: .common).autoconnect()) { date in
            store.refreshProjectFromDiskIfNeeded(now: date)
        }
    }
}

struct CommandSearchField: View {
    @ObservedObject var store: WorkbenchStore

    var body: some View {
        HStack(spacing: 6) {
            Image(systemName: "magnifyingglass")
                .foregroundStyle(.primary)
            TextField("Search or run command...", text: Binding(
                get: { store.state.commandQuery },
                set: { store.setCommandQuery($0) }
            ))
            .textFieldStyle(.plain)
            .accessibilityLabel("Search or run command")
            .onSubmit {
                store.runCommandQuery()
            }
            Text("⌘K")
                .workbenchFont(.caption)
                .foregroundStyle(.primary)
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 6)
        .frame(width: 320)
        .background(.regularMaterial)
        .clipShape(RoundedRectangle(cornerRadius: 8))
        .accessibilityIdentifier("toolbar.commandSearch")
    }
}

struct LeftDockView: View {
    @ObservedObject var store: WorkbenchStore
    @State private var datasetOrder: DatasetOrder = .alphabetical

    var body: some View {
        VStack(spacing: 0) {
            VStack(alignment: .leading, spacing: 10) {
                HStack {
                    Text(store.state.project.name)
                        .workbenchFont(.headline)
                        .lineLimit(1)

                    Spacer()

                    Button {
                        store.toggleInspector()
                    } label: {
                        Image(systemName: store.state.inspectorCollapsed ? "sidebar.right" : "sidebar.trailing")
                    }
                    .buttonStyle(.borderless)
                    .help(store.state.inspectorCollapsed ? "Show Inspector" : "Hide Inspector")
                    .accessibilityLabel(store.state.inspectorCollapsed ? "Show Inspector" : "Hide Inspector")
                    .accessibilityIdentifier(store.state.inspectorCollapsed ? "inspector.restore" : "inspector.collapse")
                }

                Text(store.state.project.rootPath)
                    .workbenchFont(.caption)
                    .foregroundStyle(.primary)
                    .lineLimit(1)

                Text(projectSourceLabel)
                    .workbenchFont(.caption2, weight: .semibold)
                    .foregroundStyle(Color(nsColor: .labelColor))

                if store.isNotebookPrototypeRuntime {
                    Text("Production boundary calls: \(store.prototypeProductionBoundaryInvocationCount)")
                        .workbenchFont(.caption, weight: .semibold, design: .monospaced)
                        .foregroundStyle(.primary)
                        .accessibilityIdentifier("notebook.boundaryAudit")
                        .accessibilityValue("\(store.prototypeProductionBoundaryInvocationCount)")
                }
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding()

            Divider()

            dockContent

            Spacer(minLength: 0)

            Divider()

            HStack {
                ForEach(DockMode.allCases) { mode in
                    Button {
                        store.selectDockMode(mode)
                    } label: {
                        VStack(spacing: 2) {
                            Image(systemName: mode.systemImage)
                            Text(mode.title)
                                .workbenchFont(.caption2)
                                .lineLimit(1)
                                .minimumScaleFactor(0.65)
                        }
                        .frame(width: 34, height: 38)
                    }
                    .buttonStyle(.borderless)
                    .help(mode.title)
                    .background(mode == store.state.dockMode ? Color.accentColor.opacity(0.14) : Color.clear)
                    .clipShape(RoundedRectangle(cornerRadius: 6))
                    .accessibilityIdentifier("dock.mode.\(mode.rawValue)")
                }
            }
            .padding(8)
        }
        .background(.regularMaterial)
    }

    @ViewBuilder
    private var dockContent: some View {
        switch store.state.dockMode {
        case .datasets:
            if store.state.project.datasets.isEmpty {
                EmptyDockState(
                    title: store.state.hasProject ? "No supported datasets found" : "No project open",
                    message: store.state.hasProject
                        ? "The project was probed, but no supported casa-rs datasets were recognized."
                        : "Open a directory to discover MeasurementSets, images, and tables.",
                    primaryActionTitle: "Open Project",
                    primarySystemImage: "folder",
                    primaryAction: {
                        if let url = ProjectOpenPanel.chooseDirectory() {
                            store.openProject(path: url.path)
                        }
                    },
                    secondaryActionTitle: "Open Demo",
                    secondarySystemImage: "shippingbox",
                    secondaryAction: {
                        store.openFixtureProject()
                    }
                )
            } else {
                VStack(spacing: 0) {
                    HStack(spacing: 8) {
                        Text("Order by")
                            .workbenchFont(.caption)
                            .foregroundStyle(.secondary)
                        Picker("Order by", selection: $datasetOrder) {
                            ForEach(DatasetOrder.allCases) { order in
                                Text(order.title).tag(order)
                            }
                        }
                        .labelsHidden()
                        .controlSize(.small)
                        Spacer()
                    }
                    .padding(.horizontal, 10)
                    .padding(.vertical, 8)

                    List(selection: Binding(
                        get: { store.state.selectedDatasetID },
                        set: { id in
                            if let id {
                                store.selectDataset(id)
                            }
                        }
                    )) {
                        if datasetOrder == .type {
                            ForEach(datasetGroups) { group in
                                Section(group.title) {
                                    ForEach(group.datasets) { dataset in
                                        datasetRow(dataset)
                                    }
                                }
                            }
                        } else if datasetOrder == .folder {
                            ForEach(datasetFolderGroups) { group in
                                Section(group.title) {
                                    ForEach(group.datasets) { dataset in
                                        datasetRow(dataset)
                                    }
                                }
                            }
                        } else {
                            ForEach(orderedDatasets) { dataset in
                                datasetRow(dataset)
                            }
                        }
                    }
                    .listStyle(.sidebar)
                    .accessibilityIdentifier("dock.datasets")
                }
            }

        case .files:
            filesDock

        case .notebooks:
            notebooksDock

        case .history:
            if store.state.history.isEmpty {
                EmptyDockState(
                    title: "No history yet",
                    message: "Opening a real project records the first timeline event.",
                    primaryActionTitle: "Open Project",
                    primarySystemImage: "folder",
                    primaryAction: {
                        if let url = ProjectOpenPanel.chooseDirectory() {
                            store.openProject(path: url.path)
                        }
                    },
                    secondaryActionTitle: "Open Demo",
                    secondarySystemImage: "shippingbox",
                    secondaryAction: {
                        store.openFixtureProject()
                    }
                )
            } else {
                List(store.state.history) { event in
                    VStack(alignment: .leading, spacing: 3) {
                        Text(event.title)
                            .workbenchFont(.subheadline)
                        Text(event.timestamp)
                            .workbenchFont(.caption)
                            .foregroundStyle(.secondary)
                        Text(event.reason)
                            .workbenchFont(.caption)
                            .foregroundStyle(.secondary)
                            .lineLimit(2)
                    }
                    .accessibilityIdentifier("history.row.\(event.id)")
                }
                .listStyle(.sidebar)
                .accessibilityIdentifier("dock.history")
            }

        }
    }

    @ViewBuilder
    private var notebooksDock: some View {
        if let tutorial = store.state.prototypeTutorial {
            let notebook = tutorial.learnerNotebook
            VStack(spacing: 0) {
                HStack {
                    Text("Project notebooks")
                        .workbenchFont(.caption, weight: .semibold)
                    Spacer()
                    Text("Tutorial")
                        .workbenchFont(.caption2, weight: .semibold)
                        .padding(.horizontal, 7)
                        .padding(.vertical, 3)
                        .background(Color.accentColor.opacity(0.12))
                        .clipShape(Capsule())
                }
                .padding(.horizontal, 12)
                .padding(.vertical, 9)

                List {
                    ForEach(notebook.notebooks) { summary in
                        Button {
                            store.openDefaultTab(kind: .notebook)
                        } label: {
                            HStack(alignment: .firstTextBaseline, spacing: 8) {
                                Image(systemName: "graduationcap.fill")
                                    .foregroundStyle(Color.accentColor)
                                VStack(alignment: .leading, spacing: 2) {
                                    Text(summary.title)
                                        .workbenchFont(.subheadline, weight: .semibold)
                                        .lineLimit(1)
                                    Text(summary.filename)
                                        .workbenchFont(.caption, design: .monospaced)
                                        .lineLimit(1)
                                }
                                Spacer(minLength: 4)
                                if notebook.isDirty {
                                    Circle().fill(.orange).frame(width: 7, height: 7)
                                        .accessibilityLabel("Unsaved changes")
                                }
                            }
                            .padding(.vertical, 3)
                        }
                        .buttonStyle(.plain)
                        .accessibilityIdentifier("notebook.selector.\(summary.id)")
                    }
                }
                .listStyle(.sidebar)
                .accessibilityIdentifier("dock.notebooks")

                Divider()
                Button {
                    store.openDefaultTab(kind: .notebook)
                } label: {
                    Label("Open learner notebook", systemImage: "arrow.up.forward.app")
                }
                .buttonStyle(.borderless)
                .padding(10)
                .accessibilityIdentifier("notebook.selector.open")
            }
        } else if let notebook = store.state.prototypeNotebook {
            VStack(spacing: 0) {
                HStack {
                    Text("Project notebooks")
                        .workbenchFont(.caption, weight: .semibold)
                        .foregroundStyle(.primary)
                    Spacer()
                }
                .padding(.horizontal, 12)
                .padding(.vertical, 9)

                List(selection: Binding(
                    get: { Optional(notebook.activeNotebookID) },
                    set: { notebookID in
                        if let notebookID {
                            store.selectPrototypeNotebook(notebookID)
                            store.openDefaultTab(kind: .notebook)
                        }
                    }
                )) {
                    ForEach(notebook.notebooks) { summary in
                        HStack(alignment: .firstTextBaseline, spacing: 8) {
                            Image(systemName: "doc.richtext")
                                .foregroundStyle(summary.id == notebook.activeNotebookID ? Color.accentColor : .secondary)
                            VStack(alignment: .leading, spacing: 2) {
                                Text(summary.title)
                                    .workbenchFont(.subheadline, weight: .semibold)
                                    .foregroundStyle(Color(nsColor: .labelColor))
                                    .lineLimit(1)
                                Text(summary.filename)
                                    .workbenchFont(.caption, design: .monospaced)
                                    .foregroundStyle(Color(nsColor: .labelColor))
                                    .lineLimit(1)
                            }
                            Spacer(minLength: 4)
                            if summary.id == notebook.activeNotebookID, notebook.isDirty {
                                Circle()
                                    .fill(.orange)
                                    .frame(width: 7, height: 7)
                                    .accessibilityLabel("Unsaved changes")
                            }
                        }
                        .padding(.vertical, 3)
                        .tag(Optional(summary.id))
                        .accessibilityIdentifier("notebook.selector.\(summary.id)")
                    }
                }
                .listStyle(.sidebar)
                .accessibilityIdentifier("dock.notebooks")

                Divider()

                HStack {
                    Label("Markdown files", systemImage: "text.document")
                        .workbenchFont(.caption, weight: .semibold)
                        .foregroundStyle(Color(nsColor: .labelColor))
                    Spacer()
                    Button {
                        store.openDefaultTab(kind: .notebook)
                    } label: {
                        Image(systemName: "arrow.up.forward.app")
                    }
                    .buttonStyle(.borderless)
                    .accessibilityLabel("Open selected notebook")
                    .help("Open selected notebook")
                    .accessibilityIdentifier("notebook.selector.open")
                }
                .padding(10)
            }
        } else if let notebooks = store.state.scientificNotebooks {
            VStack(spacing: 0) {
                List(selection: Binding(
                    get: { notebooks.activeNotebookID },
                    set: { notebookID in
                        if let notebookID {
                            store.selectScientificNotebook(notebookID)
                            store.openDefaultTab(kind: .notebook)
                        }
                    }
                )) {
                    ForEach(notebooks.notebooks) { document in
                        HStack(alignment: .firstTextBaseline, spacing: 8) {
                            Image(systemName: "doc.richtext")
                                .foregroundStyle(document.id == notebooks.activeNotebookID ? Color.accentColor : .secondary)
                            VStack(alignment: .leading, spacing: 2) {
                                Text(document.title)
                                    .workbenchFont(.subheadline, weight: .semibold)
                                    .lineLimit(1)
                                Text(document.filename)
                                    .workbenchFont(.caption, design: .monospaced)
                                    .lineLimit(1)
                            }
                            Spacer(minLength: 4)
                            if document.isDirty {
                                Circle().fill(.orange).frame(width: 7, height: 7)
                            }
                        }
                        .padding(.vertical, 3)
                        .tag(Optional(document.id))
                        .accessibilityIdentifier("notebook.selector.\(document.id)")
                    }
                }
                .listStyle(.sidebar)
                .accessibilityIdentifier("dock.notebooks")
                Divider()
                Button {
                    store.openDefaultTab(kind: .notebook)
                } label: {
                    Label("Open selected notebook", systemImage: "arrow.up.forward.app")
                }
                .buttonStyle(.borderless)
                .padding(10)
                .accessibilityIdentifier("notebook.selector.open")
                Button {
                    if notebooks.notebooks.isEmpty {
                        store.createScientificNotebook()
                    } else {
                        store.createNextNamedScientificNotebook()
                    }
                } label: {
                    Label(notebooks.notebooks.isEmpty ? "Create default notebook" : "New notebook", systemImage: "plus")
                }
                .buttonStyle(.borderless)
                .padding(.bottom, 10)
                .accessibilityIdentifier("notebook.selector.new")
            }
        } else {
            VStack(alignment: .leading, spacing: 12) {
                Text("No notebooks yet")
                    .workbenchFont(.headline)
                Text("Notebooks will appear here when the project contains Markdown notes.")
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
                Button {
                    if store.state.hasProject {
                        store.createScientificNotebook()
                    } else if let url = ProjectOpenPanel.chooseDirectory() {
                        store.openProject(path: url.path)
                    }
                } label: {
                    Label(store.state.hasProject ? "Create Notebook" : "Open Project", systemImage: store.state.hasProject ? "plus" : "folder")
                }
                .accessibilityIdentifier("dock.empty.primary")
                Spacer()
            }
            .padding()
            .frame(maxWidth: .infinity, alignment: .leading)
            .accessibilityIdentifier("dock.empty")
        }
    }

    private func datasetRow(_ dataset: DatasetSummary) -> some View {
        DatasetRow(
            dataset: dataset,
            disambiguator: datasetDisambiguator(dataset),
            isNestedInProject: datasetRelativeParent(dataset) != nil
        )
            .frame(maxWidth: .infinity, alignment: .leading)
            .contentShape(Rectangle())
            .tag(Optional(dataset.id))
            .overlay {
                DatasetRowClickTarget(
                    datasetID: dataset.id,
                    onSingleClick: {
                        store.selectDataset(dataset.id)
                    },
                    onDoubleClick: {
                        store.openDatasetExplorer(dataset.id)
                    }
                )
            }
            .contextMenu {
                Button("Open Explorer") {
                    store.openDatasetExplorer(dataset.id)
                }
                if dataset.kind == .measurementSet || dataset.kind == .table || dataset.kind == .calibrationTable {
                    Button("Open in Table Browser") {
                        store.openDatasetTableBrowser(dataset.id)
                    }
                }
            }
            .accessibilityIdentifier("dataset.row.\(dataset.id)")
    }

    private var projectSourceLabel: String {
        if store.state.isTutorialPrototype {
            return "Tutorial prototype"
        }
        if store.state.isNotebookPrototype {
            return "Notebook prototype"
        }
        return switch store.state.project.source {
        case .none: "No project"
        case .fixture: "Demo project"
        case .probed: "Real project"
        case .directMeasurementSet: "Direct MeasurementSet"
        }
    }

    private var datasetGroups: [DatasetGroup] {
        let order: [DatasetKind] = [.measurementSet, .imageCube, .region, .runProduct, .calibrationTable, .table]
        let grouped = Dictionary(grouping: store.state.project.datasets, by: \.kind)
        return order.compactMap { kind in
            guard let datasets = grouped[kind], !datasets.isEmpty else {
                return nil
            }
            return DatasetGroup(
                kind: kind,
                title: datasetGroupTitle(kind),
                datasets: datasets.sorted { $0.name.localizedStandardCompare($1.name) == .orderedAscending }
            )
        }
    }

    private var datasetFolderGroups: [DatasetFolderGroup] {
        let grouped = Dictionary(grouping: store.state.project.datasets, by: datasetFolderGroupTitle)
        return grouped.keys.sorted { lhs, rhs in
            if lhs == projectRootFolderTitle {
                return true
            }
            if rhs == projectRootFolderTitle {
                return false
            }
            return lhs.localizedStandardCompare(rhs) == .orderedAscending
        }.map { title in
            DatasetFolderGroup(
                title: title,
                datasets: (grouped[title] ?? []).sorted(by: datasetAlphabeticalSort)
            )
        }
    }

    private var orderedDatasets: [DatasetSummary] {
        switch datasetOrder {
        case .alphabetical, .type, .folder:
            store.state.project.datasets.sorted(by: datasetAlphabeticalSort)
        case .time:
            store.state.project.datasets.sorted(by: datasetTimeSort)
        }
    }

    private func datasetAlphabeticalSort(_ lhs: DatasetSummary, _ rhs: DatasetSummary) -> Bool {
        let nameOrder = lhs.name.localizedStandardCompare(rhs.name)
        if nameOrder != .orderedSame {
            return nameOrder == .orderedAscending
        }
        return lhs.path.localizedStandardCompare(rhs.path) == .orderedAscending
    }

    private func datasetTimeSort(_ lhs: DatasetSummary, _ rhs: DatasetSummary) -> Bool {
        let lhsTime = lhs.modifiedUnixSeconds ?? lhs.probedUnixSeconds ?? 0
        let rhsTime = rhs.modifiedUnixSeconds ?? rhs.probedUnixSeconds ?? 0
        if lhsTime != rhsTime {
            return lhsTime > rhsTime
        }
        return datasetAlphabeticalSort(lhs, rhs)
    }

    private func datasetGroupTitle(_ kind: DatasetKind) -> String {
        switch kind {
        case .measurementSet: "Measurement Sets"
        case .imageCube: "Images"
        case .region: "Regions"
        case .runProduct: "Products"
        case .calibrationTable: "Calibration Tables"
        case .table: "Tables"
        }
    }

    private var projectRootFolderTitle: String {
        "Project Root"
    }

    private func datasetFolderGroupTitle(_ dataset: DatasetSummary) -> String {
        datasetRelativeParent(dataset) ?? projectRootFolderTitle
    }

    private func datasetRelativeParent(_ dataset: DatasetSummary) -> String? {
        let root = store.state.project.rootPath
        guard !root.isEmpty else {
            return nil
        }
        let rootURL = URL(fileURLWithPath: root, isDirectory: true).standardizedFileURL
        let parent = URL(fileURLWithPath: dataset.path)
            .deletingLastPathComponent()
            .standardizedFileURL
        if parent.path == rootURL.path {
            return nil
        }
        let prefix = rootURL.path.hasSuffix("/") ? rootURL.path : rootURL.path + "/"
        guard parent.path.hasPrefix(prefix) else {
            return nil
        }
        let relativeParent = String(parent.path.dropFirst(prefix.count))
        return relativeParent.isEmpty ? nil : relativeParent
    }

    private func datasetDisambiguator(_ dataset: DatasetSummary) -> String? {
        let duplicateCount = store.state.project.datasets.filter { $0.name == dataset.name }.count
        guard duplicateCount > 1 else {
            return nil
        }
        let parent = URL(fileURLWithPath: dataset.path).deletingLastPathComponent().standardizedFileURL
        return datasetRelativeParent(dataset) ?? parent.lastPathComponent
    }

    @ViewBuilder
    private var filesDock: some View {
        if let notebook = store.state.prototypeNotebook {
            List {
                Section("notebooks") {
                    ForEach(notebook.notebooks) { summary in
                        Button {
                            store.selectPrototypeNotebook(summary.id)
                            store.openDefaultTab(kind: .notebook)
                        } label: {
                            Label(summary.filename, systemImage: "doc.text")
                        }
                        .buttonStyle(.plain)
                        .accessibilityIdentifier("prototypeFile.\(summary.id)")
                    }
                }
                Section("fixture data") {
                    Label("twhya_calibrated.ms", systemImage: "externaldrive")
                    Label("products", systemImage: "folder")
                }
            }
            .listStyle(.sidebar)
            .accessibilityIdentifier("dock.files.prototype")
        } else if store.state.isDemoProject {
            VStack(alignment: .leading, spacing: 12) {
                Label("data", systemImage: "folder")
                Label("calibration", systemImage: "folder")
                Label("products", systemImage: "folder")
                Label(".casa-rs-demo", systemImage: "shippingbox")
                    .foregroundStyle(.secondary)
                Spacer()
                Text("Demo project tree")
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
            }
            .padding()
            .frame(maxWidth: .infinity, alignment: .leading)
            .accessibilityIdentifier("dock.files")
        } else if store.state.hasProject {
            let nodes = ProjectFileNode.scan(
                rootPath: store.state.project.rootPath,
                datasetPaths: Set(store.state.project.datasets.map(\.path))
            )
            if nodes.isEmpty {
                EmptyDockState(
                    title: "No project files",
                    message: "The project directory is empty or could not be read.",
                    primaryActionTitle: "Refresh",
                    primarySystemImage: "arrow.clockwise",
                    primaryAction: {
                        store.refreshProjectFromDisk()
                    },
                    secondaryActionTitle: "Open Project",
                    secondarySystemImage: "folder",
                    secondaryAction: {
                        if let url = ProjectOpenPanel.chooseDirectory() {
                            store.openProject(path: url.path)
                        }
                    }
                )
            } else {
                List {
                    OutlineGroup(nodes, children: \.children) { node in
                        ProjectFileRow(node: node)
                            .accessibilityIdentifier("file.row.\(node.id)")
                    }
                }
                .listStyle(.sidebar)
                .accessibilityIdentifier("dock.files")
            }
        } else {
            EmptyDockState(
                title: "No project files",
                message: "Open a project directory to inspect its file tree.",
                primaryActionTitle: "Open Project",
                primarySystemImage: "folder",
                primaryAction: {
                    if let url = ProjectOpenPanel.chooseDirectory() {
                        store.openProject(path: url.path)
                    }
                },
                secondaryActionTitle: "Open Demo",
                secondarySystemImage: "shippingbox",
                secondaryAction: {
                    store.openFixtureProject()
                }
            )
        }
    }
}

private struct DatasetGroup: Identifiable {
    let kind: DatasetKind
    let title: String
    let datasets: [DatasetSummary]

    var id: String { kind.rawValue }
}

private struct DatasetFolderGroup: Identifiable {
    let title: String
    let datasets: [DatasetSummary]

    var id: String { title }
}

private enum DatasetOrder: String, CaseIterable, Identifiable {
    case alphabetical
    case type
    case folder
    case time

    var id: String { rawValue }

    var title: String {
        switch self {
        case .alphabetical: "Alphabetical"
        case .type: "Dataset Type"
        case .folder: "Folder"
        case .time: "Time"
        }
    }
}

private struct HorizontalResizeHandle: View {
    enum Anchor {
        case left
        case right

        var dragMultiplier: CGFloat {
            self == .left ? 1 : -1
        }
    }

    @Binding var width: CGFloat
    let range: ClosedRange<CGFloat>
    let anchor: Anchor
    let accessibilityID: String
    @State private var dragStartWidth: CGFloat?

    var body: some View {
        Rectangle()
            .fill(Color(nsColor: .separatorColor).opacity(0.8))
            .frame(width: 5)
            .contentShape(Rectangle())
            .gesture(
                DragGesture(minimumDistance: 0, coordinateSpace: .global)
                    .onChanged { value in
                        let startingWidth = dragStartWidth ?? width
                        dragStartWidth = startingWidth
                        let proposedWidth = startingWidth
                            + anchor.dragMultiplier * value.translation.width
                        width = min(max(proposedWidth, range.lowerBound), range.upperBound)
                    }
                    .onEnded { _ in
                        dragStartWidth = nil
                    }
            )
            .onHover { hovering in
                if hovering {
                    NSCursor.resizeLeftRight.push()
                } else {
                    NSCursor.pop()
                }
            }
            .accessibilityLabel("Resize panel")
            .accessibilityIdentifier(accessibilityID)
    }
}

private struct ProjectFileNode: Identifiable, Hashable {
    let id: String
    let name: String
    let path: String
    let relativePath: String
    let isDirectory: Bool
    let sizeBytes: Int?
    let children: [ProjectFileNode]?

    static func scan(rootPath: String, datasetPaths: Set<String>) -> [ProjectFileNode] {
        let rootURL = URL(fileURLWithPath: rootPath, isDirectory: true).standardizedFileURL
        let datasetDirectoryPaths = Set(datasetPaths.compactMap { path -> String? in
            var isDirectory = ObjCBool(false)
            guard FileManager.default.fileExists(atPath: path, isDirectory: &isDirectory),
                  isDirectory.boolValue
            else {
                return nil
            }
            return URL(fileURLWithPath: path).standardizedFileURL.path
        })
        var remaining = 700
        return scanDirectory(
            rootURL,
            rootURL: rootURL,
            datasetDirectoryPaths: datasetDirectoryPaths,
            depth: 0,
            remaining: &remaining
        )
    }

    private static func scanDirectory(
        _ directory: URL,
        rootURL: URL,
        datasetDirectoryPaths: Set<String>,
        depth: Int,
        remaining: inout Int
    ) -> [ProjectFileNode] {
        guard depth <= 5, remaining > 0 else {
            return []
        }
        let entries = (try? FileManager.default.contentsOfDirectory(
            at: directory,
            includingPropertiesForKeys: [.isDirectoryKey, .fileSizeKey],
            options: []
        )) ?? []
        return entries
            .filter { $0.lastPathComponent != ".DS_Store" }
            .sorted { lhs, rhs in
                let lhsIsDirectory = (try? lhs.resourceValues(forKeys: [.isDirectoryKey]).isDirectory) ?? false
                let rhsIsDirectory = (try? rhs.resourceValues(forKeys: [.isDirectoryKey]).isDirectory) ?? false
                if lhsIsDirectory != rhsIsDirectory {
                    return lhsIsDirectory
                }
                return lhs.lastPathComponent.localizedStandardCompare(rhs.lastPathComponent) == .orderedAscending
            }
            .compactMap { entry -> ProjectFileNode? in
                guard remaining > 0 else {
                    return nil
                }
                remaining -= 1
                let values = try? entry.resourceValues(forKeys: [.isDirectoryKey, .fileSizeKey])
                let isDirectory = values?.isDirectory == true
                let standardizedPath = entry.standardizedFileURL.path
                let relativePath = relativePath(for: entry, rootURL: rootURL)
                let children: [ProjectFileNode]?
                if isDirectory, !datasetDirectoryPaths.contains(standardizedPath) {
                    children = scanDirectory(
                        entry,
                        rootURL: rootURL,
                        datasetDirectoryPaths: datasetDirectoryPaths,
                        depth: depth + 1,
                        remaining: &remaining
                    )
                } else {
                    children = nil
                }
                return ProjectFileNode(
                    id: standardizedPath,
                    name: entry.lastPathComponent,
                    path: standardizedPath,
                    relativePath: relativePath,
                    isDirectory: isDirectory,
                    sizeBytes: values?.fileSize,
                    children: children
                )
            }
    }

    private static func relativePath(for url: URL, rootURL: URL) -> String {
        let rootPath = rootURL.path
        let path = url.standardizedFileURL.path
        let prefix = rootPath.hasSuffix("/") ? rootPath : rootPath + "/"
        guard path.hasPrefix(prefix) else {
            return path
        }
        return String(path.dropFirst(prefix.count))
    }
}

private struct ProjectFileRow: View {
    let node: ProjectFileNode

    var body: some View {
        Label {
            VStack(alignment: .leading, spacing: 1) {
                Text(node.name)
                    .lineLimit(1)
                Text(secondaryText)
                    .workbenchFont(.caption2)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }
        } icon: {
            Image(systemName: icon)
                .foregroundStyle(.secondary)
        }
        .help(node.relativePath)
    }

    private var secondaryText: String {
        if node.isDirectory {
            return "Folder"
        }
        if let sizeBytes = node.sizeBytes {
            return ByteCountFormatter.string(fromByteCount: Int64(sizeBytes), countStyle: .file)
        }
        return "File"
    }

    private var icon: String {
        if node.isDirectory {
            return "folder"
        }
        switch URL(fileURLWithPath: node.path).pathExtension.lowercased() {
        case "fits", "fit", "fts":
            return "doc.richtext"
        case "json":
            return "curlybraces"
        case "log", "txt", "md":
            return "doc.text"
        default:
            return "doc"
        }
    }
}

private struct DatasetRowClickTarget: NSViewRepresentable {
    let datasetID: String
    let onSingleClick: () -> Void
    let onDoubleClick: () -> Void

    func makeNSView(context: Context) -> DatasetRowClickView {
        let view = DatasetRowClickView()
        view.datasetID = datasetID
        view.onSingleClick = onSingleClick
        view.onDoubleClick = onDoubleClick
        return view
    }

    func updateNSView(_ nsView: DatasetRowClickView, context: Context) {
        nsView.datasetID = datasetID
        nsView.onSingleClick = onSingleClick
        nsView.onDoubleClick = onDoubleClick
    }
}

private final class DatasetRowClickView: NSView {
    var datasetID = ""
    var onSingleClick: (() -> Void)?
    var onDoubleClick: (() -> Void)?

    override var acceptsFirstResponder: Bool { false }

    override func mouseDown(with event: NSEvent) {
        let clickedDatasetID = datasetID
        if event.clickCount >= 2 {
            datasetClickLogger.debug("row_mouse_down double id=\(clickedDatasetID, privacy: .public)")
            onDoubleClick?()
        } else {
            datasetClickLogger.debug("row_mouse_down single id=\(clickedDatasetID, privacy: .public)")
            onSingleClick?()
        }
    }
}

struct EmptyDockState: View {
    let title: String
    let message: String
    let primaryActionTitle: String
    let primarySystemImage: String
    let primaryAction: () -> Void
    let secondaryActionTitle: String
    let secondarySystemImage: String
    let secondaryAction: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text(title)
                .workbenchFont(.headline)
            Text(message)
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
                .fixedSize(horizontal: false, vertical: true)
            Button(action: primaryAction) {
                Label(primaryActionTitle, systemImage: primarySystemImage)
            }
            .accessibilityIdentifier("dock.empty.primary")
            Button(action: secondaryAction) {
                Label(secondaryActionTitle, systemImage: secondarySystemImage)
            }
            .buttonStyle(.borderless)
            .accessibilityIdentifier("dock.empty.secondary")
            Spacer()
        }
        .padding()
        .frame(maxWidth: .infinity, alignment: .leading)
        .accessibilityIdentifier("dock.empty")
    }
}

struct DatasetRow: View {
    let dataset: DatasetSummary
    let disambiguator: String?
    let isNestedInProject: Bool

    var body: some View {
        HStack(spacing: 10) {
            Image(systemName: icon)
                .foregroundStyle(.secondary)
                .frame(width: 16)

            VStack(alignment: .leading, spacing: 2) {
                HStack(spacing: 5) {
                    Text(dataset.name)
                        .lineLimit(1)
                    if isNestedInProject {
                        Image(systemName: "folder")
                            .workbenchFont(.caption2)
                            .foregroundStyle(.secondary)
                            .help("This dataset is in a project subdirectory.")
                    }
                }
                Text(datasetSubtitle)
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }
        }
        .help(dataset.path)
    }

    private var datasetSubtitle: String {
        let base: String
        if !dataset.size.isEmpty {
            base = "\(dataset.kind.rawValue) - \(dataset.size)"
        } else {
            base = dataset.kind.rawValue
        }
        if let disambiguator, !disambiguator.isEmpty {
            return "\(base) - \(disambiguator)"
        }
        return base
    }

    private var icon: String {
        switch dataset.kind {
        case .measurementSet: "antenna.radiowaves.left.and.right"
        case .imageCube: "cube"
        case .calibrationTable: "tablecells"
        case .table: "tablecells.badge.ellipsis"
        case .region: "selection.pin.in.out"
        case .runProduct: "checkmark.seal"
        }
    }
}

struct InspectorView: View {
    @ObservedObject var store: WorkbenchStore
    @State private var showFields = false
    @State private var showSpectralWindows = false
    @State private var showAntennas = false
    @State private var showColumns = false
    @State private var showSubtables = false
    @State private var showImageLiveDetails = true
    @State private var showTableBrowserLiveDetails = true

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            HStack {
                Text("Inspector")
                    .workbenchFont(.headline)
                Spacer()
                Button {
                    store.setInspectorCollapsed(true)
                } label: {
                    Image(systemName: "xmark")
                }
                .buttonStyle(.borderless)
                .accessibilityIdentifier("inspector.close")
            }

            if let dataset = store.state.selectedDataset {
                InfoRow(label: "Name", value: dataset.name)
                InfoRow(label: "Kind", value: dataset.kind.rawValue)
                InfoRow(label: "Size", value: dataset.size)
                if dataset.sizeBytes > 0 {
                    InfoRow(label: "Bytes", value: byteCount(dataset.sizeBytes))
                }
                if !dataset.units.isEmpty {
                    InfoRow(label: "Units", value: dataset.units)
                }

                Divider()

                inspectorDetails(for: dataset)

                Divider()

                Text(dataset.notes)
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)

                Text(inspectorSourceLabel)
                    .workbenchFont(.caption2)
                    .foregroundStyle(.tertiary)
                    .padding(.top, 4)
            } else {
                Text("No dataset selected")
                    .foregroundStyle(.secondary)
            }

            Spacer()
        }
        .padding()
        .accessibilityIdentifier("inspector.panel")
        .background {
            InspectorUpdateTelemetry(dataset: store.state.selectedDataset)
        }
    }

    private var inspectorSourceLabel: String {
        if store.state.isNotebookPrototype {
            return "Prototype metadata"
        }
        return switch store.state.project.source {
        case .none: "No project"
        case .fixture: "Demo metadata"
        case .probed: "Real probe metadata"
        case .directMeasurementSet: "Direct launch metadata"
        }
    }

    @ViewBuilder
    private func inspectorDetails(for dataset: DatasetSummary) -> some View {
        switch dataset.kind {
        case .measurementSet:
            compactSection(
                title: "Fields",
                count: dataset.fields.count,
                values: dataset.fields,
                isExpanded: $showFields
            )
            compactSection(
                title: "SPWs",
                count: dataset.spectralWindows.count,
                values: dataset.spectralWindows,
                isExpanded: $showSpectralWindows
            )
            compactSection(
                title: "Antennas",
                count: dataset.antennas.count,
                values: dataset.antennas,
                isExpanded: $showAntennas
            )
            InfoRow(label: "Correlations", value: compactList(dataset.correlations))
            InfoRow(label: "Data", value: compactList(dataset.dataColumns))
            if let snapshot = store.state.tableBrowsers[dataset.id]?.snapshot {
                tableBrowserLiveDetails(snapshot)
            }

            DisclosureGroup("Columns (\(dataset.columns.count))", isExpanded: $showColumns) {
                valueList(dataset.columns)
            }
            .workbenchFont(.caption)

            DisclosureGroup("Subtables (\(dataset.subtables.count))", isExpanded: $showSubtables) {
                valueList(dataset.subtables)
            }
            .workbenchFont(.caption)

        case .imageCube:
            InfoRow(label: "Shape", value: formatShape(dataset.shape))
            imageHeaderDetails(dataset)
            if !dataset.diagnostics.isEmpty {
                DisclosureGroup("Raw image details (\(dataset.diagnostics.count))", isExpanded: $showColumns) {
                    valueList(dataset.diagnostics)
                }
                .workbenchFont(.caption)
            }
            if let snapshot = store.state.imageExplorers[dataset.id]?.snapshot {
                imageExplorerLiveDetails(snapshot)
            }

        case .region:
            InfoRow(label: "Path", value: dataset.path)
            InfoRow(label: "Use with", value: "--region \(dataset.path)")
            RegionFilePreview(path: dataset.path)
            if !dataset.diagnostics.isEmpty {
                DisclosureGroup("Region details (\(dataset.diagnostics.count))", isExpanded: $showColumns) {
                    valueList(dataset.diagnostics)
                }
                .workbenchFont(.caption)
            }

        case .calibrationTable, .table, .runProduct:
            if !dataset.shape.isEmpty {
                InfoRow(label: "Shape", value: formatShape(dataset.shape))
            }
            if let snapshot = store.state.tableBrowsers[dataset.id]?.snapshot {
                tableBrowserLiveDetails(snapshot)
            }
            if !dataset.columns.isEmpty {
                DisclosureGroup("Columns (\(dataset.columns.count))", isExpanded: $showColumns) {
                    valueList(dataset.columns)
                }
                .workbenchFont(.caption)
            }
            if !dataset.subtables.isEmpty {
                DisclosureGroup("Subtables (\(dataset.subtables.count))", isExpanded: $showSubtables) {
                    valueList(dataset.subtables)
                }
                .workbenchFont(.caption)
            }
        }
    }

    @ViewBuilder
    private func imageHeaderDetails(_ dataset: DatasetSummary) -> some View {
        let rows = humanReadableImageHeaderRows(for: dataset)
        if !rows.isEmpty {
            VStack(alignment: .leading, spacing: 8) {
                Text("Image Header")
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                ForEach(rows) { row in
                    InfoRow(label: row.label, value: row.value)
                }
            }
        }
    }

    private func tableBrowserLiveDetails(_ snapshot: TableBrowserSnapshot) -> some View {
        DisclosureGroup("Table browser", isExpanded: $showTableBrowserLiveDetails) {
            VStack(alignment: .leading, spacing: 6) {
                ForEach(tableBrowserLiveRows(snapshot)) { row in
                    CompactInfoRow(label: row.label, value: row.value)
                }
                if let inspector = snapshot.inspector {
                    Divider()
                    Text(inspector.title)
                        .workbenchFont(.caption, weight: .semibold)
                    ForEach(Array(inspector.renderedLines.prefix(8).enumerated()), id: \.offset) { _, line in
                        Text(line.isEmpty ? " " : line)
                            .workbenchFont(.caption, design: .monospaced)
                            .foregroundStyle(.secondary)
                            .lineLimit(2)
                    }
                }
            }
            .padding(.top, 4)
        }
        .workbenchFont(.caption)
    }

    private func tableBrowserLiveRows(_ snapshot: TableBrowserSnapshot) -> [InspectorDynamicLine] {
        var rows: [InspectorDynamicLine] = [
            InspectorDynamicLine(label: "View", value: snapshot.view.capitalized)
        ]
        if let address = tableBrowserAddressSummary(snapshot.selectedAddress) {
            rows.append(InspectorDynamicLine(label: "Selection", value: address))
        }
        if let vertical = snapshot.verticalMetrics {
            rows.append(InspectorDynamicLine(
                label: "Rows",
                value: "\(vertical.selectedIndex + 1)/\(vertical.totalItems)"
            ))
        }
        if let horizontal = snapshot.horizontalMetrics {
            rows.append(InspectorDynamicLine(
                label: "Columns",
                value: "\(horizontal.selectedIndex + 1)/\(horizontal.totalItems)"
            ))
        }
        return rows
    }

    private func imageExplorerLiveDetails(_ snapshot: ImageExplorerSnapshot) -> some View {
        DisclosureGroup("Live plane", isExpanded: $showImageLiveDetails) {
            VStack(alignment: .leading, spacing: 6) {
                ForEach(imageExplorerLiveRows(snapshot)) { row in
                    CompactInfoRow(label: row.label, value: row.value)
                }
            }
            .padding(.top, 4)
        }
        .workbenchFont(.caption)
    }

    private func imageExplorerLiveRows(_ snapshot: ImageExplorerSnapshot) -> [InspectorDynamicLine] {
        var rows: [InspectorDynamicLine] = []

        if let plane = snapshot.plane {
            rows.append(InspectorDynamicLine(
                label: "Plane",
                value: "\(plane.width)x\(plane.height), data \(formatImageValue(plane.dataMin, unit: plane.valueUnit))...\(formatImageValue(plane.dataMax, unit: plane.valueUnit))"
            ))
            rows.append(InspectorDynamicLine(
                label: "Clip",
                value: "\(formatImageValue(plane.clipMin, unit: plane.valueUnit))...\(formatImageValue(plane.clipMax, unit: plane.valueUnit))"
            ))
            if plane.maskedOrNonFiniteCount > 0 {
                rows.append(InspectorDynamicLine(label: "Masked", value: "\(plane.maskedOrNonFiniteCount) pixels"))
            }
        }

        if let cursor = snapshot.planeCursor {
            rows.append(InspectorDynamicLine(
                label: "Cursor",
                value: "pixel \(cursor.pixelX),\(cursor.pixelY); sampled \(cursor.sampledX),\(cursor.sampledY)"
            ))
        }

        if let probe = snapshot.probe {
            let status = probe.masked ? "masked" : (probe.finite ? "finite" : "non-finite")
            rows.append(InspectorDynamicLine(
                label: "Value",
                value: "\(formatImageValue(probe.value, unit: snapshot.plane?.valueUnit ?? "")) (\(status))"
            ))
            for axis in probe.worldAxes.prefix(3) {
                rows.append(InspectorDynamicLine(label: axis.name, value: formatImageAxisValue(axis)))
            }
        }

        if let profile = snapshot.profile {
            let selected = profile.selectedSampleIndex.map { "sample \($0)" } ?? "\(profile.samples.count) samples"
            rows.append(InspectorDynamicLine(label: "Profile", value: "\(profile.axisName), \(selected)"))
        }

        for axis in snapshot.nonDisplayAxes ?? [] {
            rows.append(InspectorDynamicLine(
                label: axis.label,
                value: "index \(axis.index + 1)/\(axis.length), pixel \(axis.pixel)"
            ))
        }

        if let defaultMask = snapshot.defaultMaskName {
            rows.append(InspectorDynamicLine(label: "Mask", value: defaultMask))
        } else if !snapshot.maskNames.isEmpty {
            rows.append(InspectorDynamicLine(label: "Masks", value: snapshot.maskNames.joined(separator: ", ")))
        }

        if let region = snapshot.region {
            var value = "\(region.shapeCount) shape(s)"
            if let stats = region.stats {
                value += ", mean \(formatImageValue(stats.mean, unit: stats.valueUnit))"
            }
            rows.append(InspectorDynamicLine(label: region.label, value: value))
        }

        return rows
    }

    private func compactSection(
        title: String,
        count: Int,
        values: [String],
        isExpanded: Binding<Bool>
    ) -> some View {
        DisclosureGroup("\(title): \(count)", isExpanded: isExpanded) {
            valueList(values)
        }
        .workbenchFont(.caption)
    }

    private func valueList(_ values: [String]) -> some View {
        VStack(alignment: .leading, spacing: 4) {
            if values.isEmpty {
                Text("None")
                    .foregroundStyle(.secondary)
            } else {
                ForEach(values, id: \.self) { value in
                    Text(value)
                        .foregroundStyle(.secondary)
                        .fixedSize(horizontal: false, vertical: true)
                }
            }
        }
        .padding(.top, 4)
    }

    private func compactList(_ values: [String]) -> String {
        if values.isEmpty {
            return "None"
        }
        if values.count <= 3 {
            return values.joined(separator: ", ")
        }
        return "\(values.count)"
    }
}

private struct InspectorUpdateTelemetry: NSViewRepresentable {
    let dataset: DatasetSummary?

    func makeNSView(context: Context) -> NSView {
        NSView(frame: .zero)
    }

    func updateNSView(_ nsView: NSView, context: Context) {
        guard let dataset else {
            inspectorLogger.debug("inspector_update empty")
            return
        }
        inspectorLogger.debug(
            "inspector_update dataset=\(dataset.id, privacy: .public) fields=\(dataset.fields.count, privacy: .public) spws=\(dataset.spectralWindows.count, privacy: .public) antennas=\(dataset.antennas.count, privacy: .public) columns=\(dataset.columns.count, privacy: .public) subtables=\(dataset.subtables.count, privacy: .public)"
        )
    }
}

private struct RegionFilePreview: View {
    let path: String

    private var inspection: RegionFileInspection? {
        RegionFileInspection.inspect(path: path)
    }

    var body: some View {
        if let inspection {
            VStack(alignment: .leading, spacing: 8) {
                Text("Region Shape")
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                Canvas { context, size in
                    drawRegion(inspection, in: &context, size: size)
                }
                .frame(height: 110)
                .background(Color(nsColor: .textBackgroundColor).opacity(0.45))
                .clipShape(RoundedRectangle(cornerRadius: 6))
                .overlay(
                    RoundedRectangle(cornerRadius: 6)
                        .stroke(Color.secondary.opacity(0.25), lineWidth: 1)
                )
                CompactInfoRow(label: "Shape", value: "\(inspection.kind), \(inspection.coordinateSystem)")
                CompactInfoRow(label: "X extent", value: inspection.xExtentLabel)
                CompactInfoRow(label: "Y extent", value: inspection.yExtentLabel)
            }
        } else {
            Text("No supported CRTF box or polygon preview available.")
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
        }
    }

    private func drawRegion(_ inspection: RegionFileInspection, in context: inout GraphicsContext, size: CGSize) {
        guard inspection.points.count >= 2,
              let minX = inspection.points.map(\.x).min(),
              let maxX = inspection.points.map(\.x).max(),
              let minY = inspection.points.map(\.y).min(),
              let maxY = inspection.points.map(\.y).max()
        else {
            return
        }
        let padding = 14.0
        let width = max(maxX - minX, 1.0)
        let height = max(maxY - minY, 1.0)
        let scale = min((size.width - padding * 2) / width, (size.height - padding * 2) / height)
        let drawnWidth = width * scale
        let drawnHeight = height * scale
        let xOffset = (size.width - drawnWidth) / 2.0
        let yOffset = (size.height - drawnHeight) / 2.0
        let points = inspection.points.map { point in
            CGPoint(
                x: xOffset + (point.x - minX) * scale,
                y: yOffset + drawnHeight - (point.y - minY) * scale
            )
        }
        var path = Path()
        path.move(to: points[0])
        for point in points.dropFirst() {
            path.addLine(to: point)
        }
        path.closeSubpath()
        context.fill(path, with: .color(.green.opacity(0.14)))
        context.stroke(path, with: .color(.green.opacity(0.9)), lineWidth: 2)
        for point in points {
            context.fill(
                Path(ellipseIn: CGRect(x: point.x - 3, y: point.y - 3, width: 6, height: 6)),
                with: .color(.green)
            )
        }
    }
}

private struct InspectorDynamicLine: Identifiable {
    let label: String
    let value: String

    var id: String { "\(label)-\(value)" }
}

private func humanReadableImageHeaderRows(for dataset: DatasetSummary) -> [InspectorDynamicLine] {
    var rows: [InspectorDynamicLine] = []

    func append(_ label: String, _ value: String?) {
        guard let value, !value.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            return
        }
        rows.append(InspectorDynamicLine(label: label, value: value))
    }

    append("Object", imageDiagnosticValue(dataset, prefix: "Object:"))
    append("Image Type", imageDiagnosticValue(dataset, prefix: "Image type:"))
    append("Pixel Type", imageDiagnosticValue(dataset, prefix: "Pixel type:"))
    append("Cell Size", imageDiagnosticValue(dataset, prefix: "Cell size:"))
    append("Center", imageDiagnosticValue(dataset, prefix: "Center:"))
    append("Frequency", imageDiagnosticValue(dataset, prefix: "Cube center frequency:"))
    append("Bandwidth", imageDiagnosticValue(dataset, prefix: "Total bandwidth:"))
    append("Channel Width", imageDiagnosticValue(dataset, prefix: "Channel separation:"))
    append("Beam", imageDiagnosticValue(dataset, prefix: "Beam:"))
    append("Masks", imageDiagnosticValue(dataset, prefix: "Masks:"))
    append("Default Mask", imageDiagnosticValue(dataset, prefix: "Default mask:"))
    append("Regions", imageDiagnosticValue(dataset, prefix: "Regions:"))

    return rows
}

private func imageDiagnosticValue(_ dataset: DatasetSummary, prefix: String) -> String? {
    dataset.diagnostics.first { line in
        line.hasPrefix(prefix)
    }?
    .dropFirst(prefix.count)
    .trimmingCharacters(in: .whitespacesAndNewlines)
}

private struct CompactInfoRow: View {
    let label: String
    let value: String

    var body: some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(label)
                .foregroundStyle(.secondary)
            Text(value.isEmpty ? "None" : value)
                .lineLimit(3)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}

struct InfoRow: View {
    let label: String
    let value: String

    var body: some View {
        VStack(alignment: .leading, spacing: 3) {
            Text(label)
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
            Text(value.isEmpty ? "None" : value)
                .workbenchFont(.subheadline)
                .lineLimit(3)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}

private func byteCount(_ bytes: UInt64) -> String {
    ByteCountFormatter.string(fromByteCount: Int64(bytes), countStyle: .file)
}

private func formatShape(_ shape: [UInt64]) -> String {
    shape.isEmpty ? "Unknown" : shape.map(String.init).joined(separator: " x ")
}

private func formatImageAxisValue(_ axis: ImageExplorerSnapshot.AxisValue) -> String {
    if isImageRightAscensionAxis(axis.name) {
        return formatImageRightAscension(axis.value)
    }
    if isImageDeclinationAxis(axis.name) {
        return formatImageDeclination(axis.value)
    }
    if let frequency = formatImageFrequency(axis.value, unit: axis.unit) {
        return frequency
    }
    return formatImageValue(axis.value, unit: axis.unit)
}

private func formatImageValue(_ value: Double, unit: String) -> String {
    let number = trimImageFloat(value)
    return unit.isEmpty ? number : "\(number) \(unit)"
}

private func trimImageFloat(_ value: Double) -> String {
    guard value.isFinite else {
        return value.isNaN ? "NaN" : "Inf"
    }
    let text = String(format: "%.5g", value)
    return text.replacingOccurrences(of: "+0", with: "+")
}

private func isImageRightAscensionAxis(_ name: String) -> Bool {
    name.compare("Right Ascension", options: .caseInsensitive) == .orderedSame
        || name.compare("RA", options: .caseInsensitive) == .orderedSame
}

private func isImageDeclinationAxis(_ name: String) -> Bool {
    name.compare("Declination", options: .caseInsensitive) == .orderedSame
        || name.compare("DEC", options: .caseInsensitive) == .orderedSame
}

private func formatImageRightAscension(_ radians: Double) -> String {
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

private func formatImageDeclination(_ radians: Double) -> String {
    let degrees = radians * 180.0 / Double.pi
    let sign = degrees < 0 ? "-" : "+"
    let absDegrees = abs(degrees)
    let wholeDegrees = Int(absDegrees)
    let minutesFloat = (absDegrees - Double(wholeDegrees)) * 60
    let minutes = Int(minutesFloat)
    let seconds = (minutesFloat - Double(minutes)) * 60
    return String(format: "%@%02d:%02d:%04.1f", sign, wholeDegrees, minutes, seconds)
}

private func formatImageFrequency(_ value: Double, unit: String) -> String? {
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
    return "\(trimImageFloat(value / scale)) \(suffix)"
}
