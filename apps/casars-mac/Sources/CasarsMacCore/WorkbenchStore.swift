import Foundation
import CasarsFrontendServices

public protocol ProjectProbeClient {
    func probeProject(path: String) throws -> ProjectFixtureProbe
}

public struct ProjectFixtureProbe: Equatable {
    public var project: ProjectFixture
    public var diagnostics: [String]

    public init(project: ProjectFixture, diagnostics: [String]) {
        self.project = project
        self.diagnostics = diagnostics
    }
}

public struct UniFFIProjectProbeClient: ProjectProbeClient {
    public init() {}

    public func probeProject(path: String) throws -> ProjectFixtureProbe {
        let probe = try CasarsFrontendServices.probeProject(path: path)
        return ProjectFixtureProbe(project: ProjectFixture(probe: probe), diagnostics: probe.diagnostics)
    }
}

public final class WorkbenchStore: ObservableObject {
    @Published public private(set) var state: WorkbenchState
    private let probeClient: ProjectProbeClient

    public init(
        state: WorkbenchState = FixtureWorkbench.makeState(),
        probeClient: ProjectProbeClient = UniFFIProjectProbeClient()
    ) {
        self.state = state
        self.probeClient = probeClient
    }

    public static func fixture() -> WorkbenchStore {
        WorkbenchStore(state: FixtureWorkbench.makeState())
    }

    public func openFixtureProject() {
        let interfaceFontSize = state.interfaceFontSize
        state = FixtureWorkbench.makeState()
        state.interfaceFontSize = interfaceFontSize
    }

    public func openProject(path: String) {
        do {
            let probed = try probeClient.probeProject(path: path)
            state.project = probed.project
            state.probeDiagnostics = probed.diagnostics
            state.selectedDatasetID = probed.project.datasets.first?.id
            state.dockMode = .datasets
            state.leftDockCollapsed = false
            state.inspectorCollapsed = false
            state.tabs = []
            state.activeTabID = ""
            if let dataset = state.selectedDataset {
                openExplorer(for: dataset)
            }
            state.history.append(
                ProcessingHistoryEvent(
                    id: "hist-project-open-\(state.history.count + 1)",
                    timestamp: "probed",
                    title: "Project opened",
                    reason: "Opened real project directory and probed datasets with Rust frontend services.",
                    affectedPaths: [probed.project.rootPath],
                    approval: "user"
                )
            )
        } catch {
            state.lastErrors.append("Open project \(path): \(error)")
        }
    }

    public func selectDockMode(_ mode: DockMode) {
        state.dockMode = mode
        state.leftDockCollapsed = false
    }

    public func selectDataset(_ datasetID: String) {
        guard state.project.datasets.contains(where: { $0.id == datasetID }) else {
            state.lastErrors.append("Unknown dataset \(datasetID)")
            return
        }

        state.selectedDatasetID = datasetID
    }

    public func openSelectedDatasetExplorer() {
        guard let dataset = state.selectedDataset else {
            state.lastErrors.append("No selected dataset to explore")
            return
        }

        openExplorer(for: dataset)
    }

    public func openDatasetExplorer(_ datasetID: String) {
        guard let dataset = state.project.datasets.first(where: { $0.id == datasetID }) else {
            state.lastErrors.append("Unknown dataset \(datasetID)")
            return
        }

        state.selectedDatasetID = datasetID
        openExplorer(for: dataset)
    }

    public func setInspectorCollapsed(_ collapsed: Bool) {
        state.inspectorCollapsed = collapsed
    }

    public func toggleInspector() {
        state.inspectorCollapsed.toggle()
    }

    public func setLeftDockCollapsed(_ collapsed: Bool) {
        state.leftDockCollapsed = collapsed
    }

    public func toggleLeftDock() {
        state.leftDockCollapsed.toggle()
    }

    public func setCommandQuery(_ query: String) {
        state.commandQuery = query
    }

    public func runCommandQuery() {
        let query = state.commandQuery.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !query.isEmpty else {
            openDefaultTab(kind: .aiChat)
            return
        }

        let normalized = query.lowercased()
        if normalized.contains("python") {
            openDefaultTab(kind: .python)
        } else if normalized.contains("history") || normalized.contains("timeline") {
            openDefaultTab(kind: .history)
            selectDockMode(.history)
        } else if normalized.contains("left dock") || normalized.contains("sidebar") {
            setLeftDockCollapsed(false)
        } else if normalized.contains("task") || normalized.contains("calibrate") {
            openDefaultTab(kind: .task)
        } else if normalized.contains("inspector") {
            setInspectorCollapsed(false)
        } else if normalized.contains("dataset") || normalized.contains("ms") {
            openDefaultTab(kind: .datasetExplorer)
            selectDockMode(.datasets)
        } else {
            appendAIChatMessage(query)
            openDefaultTab(kind: .aiChat)
        }
    }

    public func openTab(_ tab: WorkbenchTab) {
        if !state.tabs.contains(where: { $0.id == tab.id }) {
            state.tabs.append(tab)
        }
        state.activeTabID = tab.id
    }

    public func activateTab(_ tabID: String) {
        guard state.tabs.contains(where: { $0.id == tabID }) else {
            state.lastErrors.append("Unknown tab \(tabID)")
            return
        }
        state.activeTabID = tabID
    }

    public func openDefaultTab(kind: WorkbenchTabKind) {
        switch kind {
        case .datasetExplorer:
            openSelectedDatasetExplorer()
        case .task:
            openTab(WorkbenchTab(id: "tab-task", title: "Calibrate", kind: .task, datasetID: state.selectedDatasetID))
        case .aiChat:
            openTab(WorkbenchTab(id: "tab-ai", title: "AI Chat", kind: .aiChat))
        case .python:
            openTab(WorkbenchTab(id: "tab-python", title: "Python", kind: .python))
        case .history:
            openTab(WorkbenchTab(id: "tab-history", title: "History", kind: .history))
        }
    }

    public func applyAIProposal(_ proposalID: String) {
        guard let index = state.aiProposals.firstIndex(where: { $0.id == proposalID }) else {
            state.lastErrors.append("Unknown AI proposal \(proposalID)")
            return
        }

        state.aiProposals[index].state = .applied
        let proposal = state.aiProposals[index]
        if proposal.parameterName == "Spectral window" {
            state.taskParameters.selectedSpectralWindow = proposal.newValue
        }
        state.history.append(
            ProcessingHistoryEvent(
                id: "hist-\(proposalID)-applied",
                timestamp: "2026-05-04 09:21",
                title: "AI proposal applied",
                reason: proposal.detail,
                affectedPaths: ["task/calibrate.request"],
                approval: "user"
            )
        )
    }

    public func rejectAIProposal(_ proposalID: String) {
        guard let index = state.aiProposals.firstIndex(where: { $0.id == proposalID }) else {
            state.lastErrors.append("Unknown AI proposal \(proposalID)")
            return
        }

        state.aiProposals[index].state = .rejected
    }

    public func appendAIChatMessage(_ text: String, author: ChatAuthor = .user) {
        let id = "msg-\(state.aiMessages.count + 1)"
        state.aiMessages.append(AIChatMessage(id: id, author: author, text: text))
    }

    public func setTaskSpectralWindow(_ spectralWindow: String) {
        state.taskParameters.selectedSpectralWindow = spectralWindow
    }

    public func runTask() {
        state.taskRun = TaskRun(
            state: .completed,
            progress: 1.0,
            logLines: [
                "Started fixture calibrate dry run.",
                "Resolved field \(state.taskParameters.selectedField).",
                "Resolved spectral window \(state.taskParameters.selectedSpectralWindow).",
                "Recorded fixture product \(state.taskParameters.outputName)."
            ],
            warnings: ["Fixture run: no science data was modified."],
            products: ["project/products/\(state.taskParameters.outputName)"]
        )
        state.history.append(
            ProcessingHistoryEvent(
                id: "hist-run-\(state.history.count + 1)",
                timestamp: "2026-05-04 09:24",
                title: "Fixture task completed",
                reason: "User ran the dry-run task from the task tab.",
                affectedPaths: state.taskRun.products,
                approval: "user"
            )
        )
    }

    public func stopTask() {
        state.taskRun.state = .stopped
        state.taskRun.logLines.append("Stopped fixture task.")
    }

    public func setPythonOwner(_ owner: PythonOwner) {
        state.python.owner = owner
    }

    public func setInterfaceFontSize(_ size: Double) {
        state.interfaceFontSize = WorkbenchState.clampedInterfaceFontSize(size)
    }

    public func adjustInterfaceFontSize(by delta: Double) {
        setInterfaceFontSize(state.interfaceFontSize + delta)
    }

    public func resetInterfaceFontSize() {
        setInterfaceFontSize(WorkbenchState.defaultInterfaceFontSize)
    }

    public func debugSnapshot() -> DebugStateSnapshot {
        DebugStateSnapshot(state: state)
    }

    public func debugJSON(pretty: Bool = true) throws -> String {
        let encoder = JSONEncoder()
        if pretty {
            encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        }
        let data = try encoder.encode(debugSnapshot())
        return String(decoding: data, as: UTF8.self)
    }

    private func openExplorer(for dataset: DatasetSummary) {
        openTab(
            WorkbenchTab(
                id: dataset.explorerTabID,
                title: dataset.explorerTabTitle,
                kind: .datasetExplorer,
                datasetID: dataset.id
            )
        )
    }
}

extension ProjectFixture {
    init(probe: CasarsFrontendServices.ProjectProbe) {
        self.init(
            name: probe.name,
            rootPath: probe.rootPath,
            datasets: probe.datasets.map(DatasetSummary.init(probe:)),
            source: .probed
        )
    }
}

extension DatasetSummary {
    init(probe: CasarsFrontendServices.DatasetProbe) {
        self.init(
            id: probe.id,
            name: probe.name,
            path: probe.path,
            kind: DatasetKind(probeKind: probe.kind),
            size: probe.logicalSize,
            units: probe.units,
            fields: probe.fields,
            spectralWindows: probe.spectralWindows,
            scans: probe.scans,
            notes: probe.notes
        )
    }
}

extension DatasetKind {
    init(probeKind: CasarsFrontendServices.DatasetKind) {
        switch probeKind {
        case .measurementSet:
            self = .measurementSet
        case .image:
            self = .imageCube
        case .table:
            self = .table
        }
    }
}
