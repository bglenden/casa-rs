import CasarsMacCore
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
                Button {
                    store.activateTab(tab.id)
                } label: {
                    HStack(spacing: 6) {
                        Image(systemName: icon(for: tab.kind))
                        Text(tab.title)
                            .lineLimit(1)
                    }
                    .padding(.horizontal, 10)
                    .padding(.vertical, 7)
                    .background(tab.id == store.state.activeTabID ? Color.accentColor.opacity(0.14) : Color.clear)
                    .clipShape(RoundedRectangle(cornerRadius: 6))
                }
                .buttonStyle(.borderless)
                .accessibilityIdentifier("central.tab.\(tab.id)")
            }

            Menu {
                Button("Dataset Explorer") {
                    store.openDefaultTab(kind: .datasetExplorer)
                }
                Button("Calibrate Task") {
                    store.openDefaultTab(kind: .task)
                }
                Button("AI Chat") {
                    store.openDefaultTab(kind: .aiChat)
                }
                Button("Python") {
                    store.openDefaultTab(kind: .python)
                }
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
        let tab = store.state.tabs.first { $0.id == store.state.activeTabID }
        switch tab?.kind {
        case .datasetExplorer:
            DatasetExplorerPanel(store: store)
        case .task:
            TaskPanel(store: store)
        case .aiChat:
            AIChatPanel(store: store)
        case .python:
            PythonPanel(store: store)
        case .history:
            HistoryPanel(store: store)
        case .none:
            Text("No active tab")
                .foregroundStyle(.secondary)
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

struct DatasetExplorerPanel: View {
    @ObservedObject var store: WorkbenchStore

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 18) {
                PanelHeader(title: store.state.selectedDataset?.name ?? "Dataset Explorer", subtitle: "Fixture explorer: summary, fields, spectral windows, scans, and preview plots")

                HStack(alignment: .top, spacing: 16) {
                    SummaryBox(title: "Fields", values: store.state.selectedDataset?.fields ?? [])
                    SummaryBox(title: "Spectral windows", values: store.state.selectedDataset?.spectralWindows ?? [])
                    SummaryBox(title: "Scans", values: store.state.selectedDataset?.scans ?? [])
                }

                HStack(spacing: 16) {
                    PlotPlaceholder(title: "Amplitude vs. channel", caption: "spw 1, target field")
                    PlotPlaceholder(title: "UV distance", caption: "fixture visibility sample")
                }

                Text("Fixture/demo data")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            .padding(20)
        }
        .accessibilityIdentifier("panel.datasetExplorer")
    }
}

struct TaskPanel: View {
    @ObservedObject var store: WorkbenchStore

    var body: some View {
        VStack(spacing: 0) {
            HStack {
                VStack(alignment: .leading, spacing: 2) {
                    Text("Calibrate")
                        .font(.title3)
                        .fontWeight(.semibold)
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
                .font(.headline)
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
                .font(.headline)

            ForEach(store.state.aiProposals) { proposal in
                VStack(alignment: .leading, spacing: 8) {
                    Text(proposal.title)
                        .font(.subheadline)
                        .fontWeight(.semibold)
                    Text("\(proposal.parameterName): \(proposal.oldValue) -> \(proposal.newValue)")
                        .font(.caption)
                    Text(proposal.detail)
                        .font(.caption)
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
                            .font(.caption)
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
                .font(.headline)
            ProgressView(value: store.state.taskRun.progress)
            Text(store.state.taskRun.state.rawValue)
                .font(.caption)
                .foregroundStyle(.secondary)
            ForEach(store.state.taskRun.logLines, id: \.self) { line in
                Text(line)
                    .font(.system(.caption, design: .monospaced))
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
                        .font(.caption)
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
                .font(.caption)
                .foregroundStyle(store.state.python.owner == .ai ? .orange : .secondary)
                .accessibilityIdentifier("python.ownershipState")

            TextEditor(text: .constant(store.state.python.buffer))
                .font(.system(.body, design: .monospaced))
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
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        Text(event.title)
                            .font(.headline)
                        Text(event.reason)
                        Text("Approval: \(event.approval)")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                        ForEach(event.affectedPaths, id: \.self) { path in
                            Text(path)
                                .font(.system(.caption, design: .monospaced))
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
                .font(.title2)
                .fontWeight(.semibold)
            Text(subtitle)
                .font(.subheadline)
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
                .font(.headline)
            ForEach(values.isEmpty ? ["None"] : values, id: \.self) { value in
                Text(value)
                    .font(.caption)
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
                .font(.headline)
            ZStack {
                RoundedRectangle(cornerRadius: 8)
                    .fill(Color.accentColor.opacity(0.10))
                VStack(spacing: 10) {
                    Image(systemName: "waveform.path.ecg.rectangle")
                        .font(.largeTitle)
                    Text(caption)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
            .frame(height: 180)
        }
        .frame(maxWidth: .infinity)
    }
}
