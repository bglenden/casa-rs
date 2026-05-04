import CasarsMacCore
import SwiftUI

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
                .font(.caption)
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
                        .font(.headline)
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
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
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
                                .font(.caption2)
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
                        .tag(Optional(dataset.id))
                        .accessibilityIdentifier("dataset.row.\(dataset.id)")
                }
            }
            .listStyle(.sidebar)
            .accessibilityIdentifier("dock.datasets")

        case .files:
            VStack(alignment: .leading, spacing: 12) {
                Label("data", systemImage: "folder")
                Label("calibration", systemImage: "folder")
                Label("products", systemImage: "folder")
                Label(".casa-rs-demo", systemImage: "shippingbox")
                    .foregroundStyle(.secondary)
                Spacer()
                Text("Fixture project tree")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            .padding()
            .frame(maxWidth: .infinity, alignment: .leading)
            .accessibilityIdentifier("dock.files")

        case .history:
            List(store.state.history) { event in
                VStack(alignment: .leading, spacing: 3) {
                    Text(event.title)
                        .font(.subheadline)
                    Text(event.timestamp)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Text(event.reason)
                        .font(.caption)
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
                    .font(.caption)
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

    var body: some View {
        VStack(alignment: .leading, spacing: 14) {
            HStack {
                Text("Inspector")
                    .font(.headline)
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
                InfoRow(label: "Units", value: dataset.units)
                InfoRow(label: "Fields", value: dataset.fields.joined(separator: ", "))
                InfoRow(label: "SPWs", value: dataset.spectralWindows.joined(separator: ", "))

                Divider()

                Text(dataset.notes)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)

                Text("Fixture/demo metadata")
                    .font(.caption2)
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
    }
}

struct InfoRow: View {
    let label: String
    let value: String

    var body: some View {
        VStack(alignment: .leading, spacing: 3) {
            Text(label)
                .font(.caption)
                .foregroundStyle(.secondary)
            Text(value.isEmpty ? "None" : value)
                .font(.subheadline)
                .lineLimit(3)
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}
