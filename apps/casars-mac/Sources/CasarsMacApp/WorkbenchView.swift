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

    var body: some View {
        HStack(spacing: 0) {
            if !store.state.leftDockCollapsed {
                LeftDockView(store: store)
                    .frame(width: 250)

                Divider()
            }

            if !store.state.inspectorCollapsed {
                InspectorView(store: store)
                    .frame(width: 250)

                Divider()
            }

            CentralWorkspaceView(store: store)
                .frame(minWidth: 560)
        }
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
    }
}

struct CommandSearchField: View {
    @ObservedObject var store: WorkbenchStore

    var body: some View {
        HStack(spacing: 6) {
            Image(systemName: "magnifyingglass")
                .foregroundStyle(.secondary)
            TextField("Search or run command...", text: Binding(
                get: { store.state.commandQuery },
                set: { store.setCommandQuery($0) }
            ))
            .textFieldStyle(.plain)
            .onSubmit {
                store.runCommandQuery()
            }
            Text("⌘K")
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
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
                    .foregroundStyle(.secondary)
                    .lineLimit(1)

                Text(projectSourceLabel)
                    .workbenchFont(.caption2)
                    .foregroundStyle(.tertiary)
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
                List(selection: Binding(
                    get: { store.state.selectedDatasetID },
                    set: { id in
                        if let id {
                            store.selectDataset(id)
                        }
                    }
                )) {
                    ForEach(store.state.project.datasets) { dataset in
                        DatasetRow(dataset: dataset)
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
                            .accessibilityIdentifier("dataset.row.\(dataset.id)")
                    }
                }
                .listStyle(.sidebar)
                .accessibilityIdentifier("dock.datasets")
            }

        case .files:
            filesDock

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

    private var projectSourceLabel: String {
        switch store.state.project.source {
        case .none: "No project"
        case .fixture: "Demo project"
        case .probed: "Real project"
        }
    }

    @ViewBuilder
    private var filesDock: some View {
        if store.state.isDemoProject {
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
            List(store.state.project.datasets) { dataset in
                Label {
                    VStack(alignment: .leading, spacing: 2) {
                        Text(dataset.name)
                            .lineLimit(1)
                        Text(dataset.path)
                            .workbenchFont(.caption)
                            .foregroundStyle(.secondary)
                            .lineLimit(1)
                    }
                } icon: {
                    Image(systemName: "doc")
                }
                .accessibilityIdentifier("file.row.\(dataset.id)")
            }
            .listStyle(.sidebar)
            .accessibilityIdentifier("dock.files")
        } else {
            EmptyDockState(
                title: "No project files",
                message: "Open a project directory to inspect its recognized datasets.",
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

    var body: some View {
        HStack(spacing: 10) {
            Image(systemName: icon)
                .foregroundStyle(.secondary)
                .frame(width: 16)

            VStack(alignment: .leading, spacing: 2) {
                Text(dataset.name)
                    .lineLimit(1)
                Text(dataset.path)
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }
        }
    }

    private var icon: String {
        switch dataset.kind {
        case .measurementSet: "antenna.radiowaves.left.and.right"
        case .imageCube: "cube"
        case .calibrationTable: "tablecells"
        case .table: "tablecells.badge.ellipsis"
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
        switch store.state.project.source {
        case .none: "No project"
        case .fixture: "Demo metadata"
        case .probed: "Real probe metadata"
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
            InfoRow(label: "Pixel type", value: dataset.units)
            if !dataset.diagnostics.isEmpty {
                DisclosureGroup("Image details (\(dataset.diagnostics.count))", isExpanded: $showColumns) {
                    valueList(dataset.diagnostics)
                }
                .workbenchFont(.caption)
            }

        case .calibrationTable, .table, .runProduct:
            if !dataset.shape.isEmpty {
                InfoRow(label: "Shape", value: formatShape(dataset.shape))
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
