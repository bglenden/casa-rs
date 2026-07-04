import CasarsMacCore
import AppKit
import Darwin
import Foundation
import SwiftUI

@main
struct CasarsMacApp: App {
    private static let interfaceFontSizeKey = "interfaceFontSize"

    @NSApplicationDelegateAdaptor(AppDelegate.self) private var appDelegate
    @AppStorage(Self.interfaceFontSizeKey) private var interfaceFontSize = WorkbenchState.defaultInterfaceFontSize
    @StateObject private var store = WorkbenchStore.empty()
    @State private var didOpenStartupProject = false
    private let startupProjectPath: String?
    private let startupImagerMeasurementSetPath: String?
    private let startupTutorialPackPath: String?
    private let startupTutorialSectionID: String?
    private let startupOpenSelectedDatasetExplorer: Bool
    private let startupImageRegionBoxes: [(Int, Int, Int, Int)]
    private let startupImageRegionExportPath: String?
    private let startupShowImagerProgressMockup: Bool
    private let startupOpenImagerTask: Bool
    private let startupRunActiveTask: Bool

    init() {
        let arguments = CommandLine.arguments
        if arguments.contains("--capture-gui-evidence") {
            Self.captureGUIEvidence(arguments: arguments)
            exit(0)
        }
        if arguments.contains("--dump-debug-state") {
            Self.dumpDebugState(
                simulateMainFlow: arguments.contains("--simulate-main-flow"),
                tutorialPackPath: Self.argumentValue(after: "--open-tutorial-pack", in: arguments),
                tutorialSectionID: Self.argumentValue(after: "--open-tutorial-section", in: arguments),
                taskValueOverrides: Self.argumentPairs(after: "--set-task-value", in: arguments),
                taskToggleOverrides: Self.argumentPairs(after: "--set-task-toggle", in: arguments),
                runActiveTask: arguments.contains("--run-active-task"),
                openSelectedDatasetExplorer: arguments.contains("--open-selected-dataset-explorer"),
                imageRegionBoxes: Self.regionBoxes(after: "--image-region-box", in: arguments),
                imageRegionExportPath: Self.argumentValue(after: "--export-image-region-file", in: arguments),
                showImagerProgressMockup: arguments.contains("--show-imager-progress-mockup"),
                imagerMeasurementSetPath: Self.argumentValue(after: "--open-imager-ms", in: arguments),
                projectPath: Self.argumentValue(after: "--probe-project", in: arguments)
                    ?? Self.argumentValue(after: "--open-project", in: arguments)
            )
            exit(0)
        }

        startupProjectPath = Self.argumentValue(after: "--open-project", in: arguments)
            ?? Self.argumentValue(after: "--probe-project", in: arguments)
        startupImagerMeasurementSetPath = Self.argumentValue(after: "--open-imager-ms", in: arguments)
        startupTutorialPackPath = Self.argumentValue(after: "--open-tutorial-pack", in: arguments)
        startupTutorialSectionID = Self.argumentValue(after: "--open-tutorial-section", in: arguments)
        startupOpenSelectedDatasetExplorer = arguments.contains("--open-selected-dataset-explorer")
        startupImageRegionBoxes = Self.regionBoxes(after: "--image-region-box", in: arguments)
        startupImageRegionExportPath = Self.argumentValue(after: "--export-image-region-file", in: arguments)
        startupShowImagerProgressMockup = arguments.contains("--show-imager-progress-mockup")
        startupOpenImagerTask = arguments.contains("--open-imager-task")
        startupRunActiveTask = arguments.contains("--run-active-task")
    }

    var body: some Scene {
        WindowGroup("casa-rs Workbench") {
            WorkbenchView(store: store)
                .frame(minWidth: 1120, minHeight: 720)
                .environment(\.workbenchFontSize, store.state.interfaceFontSize)
                .background(WindowConfigurationView())
                .onAppear {
                    syncStoreFontSizeFromSettings()
                    openStartupProjectIfNeeded()
                }
                .onChange(of: interfaceFontSize) { newValue in
                    store.setInterfaceFontSize(newValue)
                }
                .onChange(of: store.state.interfaceFontSize) { newValue in
                    interfaceFontSize = newValue
                }
        }
        .commands {
            CommandMenu("Workbench") {
                Button("Open Project Directory...") {
                    if let url = ProjectOpenPanel.chooseDirectory() {
                        store.openProject(path: url.path)
                    }
                }
                .keyboardShortcut("o", modifiers: [.command])

                Button("Open Tutorial Pack...") {
                    if let url = TutorialPackOpenPanel.choosePack() {
                        store.openTutorialPack(path: url.path)
                    }
                }
                .keyboardShortcut("t", modifiers: [.command, .shift])

                Button("Open Demo Project") {
                    store.openFixtureProject()
                }
                .keyboardShortcut("o", modifiers: [.command, .shift])

                Button("Open AI Chat") {
                    store.openDefaultTab(kind: .aiChat)
                }
                .keyboardShortcut("l", modifiers: [.command, .shift])

                Button("Close Active Tab") {
                    store.closeActiveTab()
                }
                .keyboardShortcut("w", modifiers: [.command])
                .disabled(store.state.tabs.isEmpty)

                Button("Toggle Inspector") {
                    store.toggleInspector()
                }
                .keyboardShortcut("i", modifiers: [.command, .option])

                Button(store.state.leftDockCollapsed ? "Show Left Dock" : "Hide Left Dock") {
                    store.toggleLeftDock()
                }
                .keyboardShortcut("s", modifiers: [.command, .option])

                Button(store.state.inspectorCollapsed ? "Show Inspector" : "Hide Inspector") {
                    store.toggleInspector()
                }
                .keyboardShortcut("i", modifiers: [.command, .shift])

                Button("Run Command Query") {
                    store.runCommandQuery()
                }
                .keyboardShortcut("k", modifiers: [.command])
            }

            CommandMenu("Window Layout") {
                Button("Toggle Full Screen") {
                    FullScreenController.toggleFullScreen()
                }
                .keyboardShortcut("f", modifiers: [.command, .control])
            }

            CommandMenu("Display") {
                Button("Increase Font Size") {
                    setStoredInterfaceFontSize(interfaceFontSize + 1)
                }
                .keyboardShortcut("+", modifiers: [.command])
                .disabled(interfaceFontSize >= WorkbenchState.maximumInterfaceFontSize)

                Button("Decrease Font Size") {
                    setStoredInterfaceFontSize(interfaceFontSize - 1)
                }
                .keyboardShortcut("-", modifiers: [.command])
                .disabled(interfaceFontSize <= WorkbenchState.minimumInterfaceFontSize)

                Button("Reset Font Size") {
                    setStoredInterfaceFontSize(WorkbenchState.defaultInterfaceFontSize)
                }
                .keyboardShortcut("0", modifiers: [.command])
            }
        }
        Settings {
            DisplaySettingsView(interfaceFontSize: $interfaceFontSize)
        }
    }

    private static func dumpDebugState(
        simulateMainFlow: Bool,
        tutorialPackPath: String?,
        tutorialSectionID: String?,
        taskValueOverrides: [(String, String)],
        taskToggleOverrides: [(String, String)],
        runActiveTask: Bool,
        openSelectedDatasetExplorer: Bool,
        imageRegionBoxes: [(Int, Int, Int, Int)],
        imageRegionExportPath: String?,
        showImagerProgressMockup: Bool,
        imagerMeasurementSetPath: String?,
        projectPath: String?
    ) {
        let store = WorkbenchStore.empty()
        store.setInterfaceFontSize(storedInterfaceFontSize())
        if let tutorialPackPath {
            store.openTutorialPack(path: tutorialPackPath)
            if let tutorialSectionID {
                store.openTutorialSectionTask(tutorialSectionID)
            }
        } else if let imagerMeasurementSetPath {
            store.openExternalMeasurementSetForDirtyImaging(path: imagerMeasurementSetPath)
        } else if let projectPath {
            store.openProject(path: projectPath)
        }
        for (argumentID, value) in taskValueOverrides {
            store.setGenericTaskValue(argumentID: argumentID, value: value)
        }
        for (argumentID, value) in taskToggleOverrides {
            store.setGenericTaskToggle(argumentID: argumentID, value: value == "true")
        }
        if runActiveTask {
            store.setGenericTaskConfirmation(taskID: store.state.activeTaskID, confirmed: true)
            store.runTask()
            Self.waitForTaskToFinish(store: store, timeoutSeconds: 120)
        }
        if openSelectedDatasetExplorer || !imageRegionBoxes.isEmpty {
            store.openDefaultTab(kind: .datasetExplorer)
            Self.applyImageRegionBoxes(imageRegionBoxes, store: store)
            Self.exportImageRegionIfNeeded(imageRegionExportPath, store: store)
        }
        if showImagerProgressMockup {
            store.openImagerProgressMockup()
        }
        if simulateMainFlow {
            if tutorialPackPath == nil && projectPath == nil {
                store.openFixtureProject()
            }
            if store.state.isDemoProject {
                store.selectDockMode(.history)
                store.setInspectorCollapsed(true)
                store.applyAIProposal("proposal-spw")
                store.setPythonOwner(.ai)
                store.runTask()
                store.openDefaultTab(kind: .plotSamples)
                store.openDefaultTab(kind: .history)
            } else if let dataset = store.state.selectedDataset, dataset.kind == .measurementSet {
                store.runMeasurementSetPlot(datasetID: dataset.id)
                store.openDefaultTab(kind: .task)
                store.runTask()
                Self.waitForTaskToFinish(store: store, timeoutSeconds: 120)
            }
        }

        do {
            let json = try store.debugJSON()
            print(json)
        } catch {
            fputs("failed to encode debug state: \(error)\n", stderr)
            exit(1)
        }
    }

    private static func applyImageRegionBoxes(
        _ boxes: [(Int, Int, Int, Int)],
        store: WorkbenchStore
    ) {
        guard let datasetID = store.state.selectedDatasetID else { return }
        for (x0, y0, x1, y1) in boxes {
            store.appendImageExplorerRegionCommand(.startRegionShape, datasetID: datasetID)
            store.appendImageExplorerRegionCommand(.appendRegionVertex(x: x0, y: y0), datasetID: datasetID)
            store.appendImageExplorerRegionCommand(.appendRegionVertex(x: x1, y: y0), datasetID: datasetID)
            store.appendImageExplorerRegionCommand(.appendRegionVertex(x: x1, y: y1), datasetID: datasetID)
            store.appendImageExplorerRegionCommand(.appendRegionVertex(x: x0, y: y1), datasetID: datasetID)
            store.appendImageExplorerRegionCommand(.closeRegionShape, datasetID: datasetID)
        }
    }

    private static func exportImageRegionIfNeeded(_ path: String?, store: WorkbenchStore) {
        guard let path else { return }
        guard let datasetID = store.state.selectedDatasetID else { return }
        store.exportImageExplorerRegionFile(datasetID: datasetID, path: path)
    }

    private static func argumentValue(after flag: String, in arguments: [String]) -> String? {
        guard let index = arguments.firstIndex(of: flag), arguments.indices.contains(index + 1) else {
            return nil
        }
        return arguments[index + 1]
    }

    private static func argumentPairs(after flag: String, in arguments: [String]) -> [(String, String)] {
        var pairs: [(String, String)] = []
        var index = arguments.startIndex
        while index < arguments.endIndex {
            guard arguments[index] == flag else {
                index = arguments.index(after: index)
                continue
            }
            let keyIndex = arguments.index(after: index)
            let valueIndex = arguments.index(keyIndex, offsetBy: 1)
            guard keyIndex < arguments.endIndex, valueIndex < arguments.endIndex else {
                break
            }
            pairs.append((arguments[keyIndex], arguments[valueIndex]))
            index = arguments.index(after: valueIndex)
        }
        return pairs
    }

    private static func regionBoxes(after flag: String, in arguments: [String]) -> [(Int, Int, Int, Int)] {
        var boxes: [(Int, Int, Int, Int)] = []
        var index = arguments.startIndex
        while index < arguments.endIndex {
            guard arguments[index] == flag else {
                index = arguments.index(after: index)
                continue
            }
            let valueIndex = arguments.index(after: index)
            guard valueIndex < arguments.endIndex else { break }
            let parts = arguments[valueIndex].split(separator: ",").compactMap { Int($0.trimmingCharacters(in: .whitespaces)) }
            if parts.count == 4 {
                boxes.append((parts[0], parts[1], parts[2], parts[3]))
            }
            index = arguments.index(after: valueIndex)
        }
        return boxes
    }

    private static func waitForTaskToFinish(store: WorkbenchStore, timeoutSeconds: TimeInterval) {
        let deadline = Date().addingTimeInterval(timeoutSeconds)
        while store.state.taskRun.state == .running && Date() < deadline {
            RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.05))
        }
    }

    private static func waitForMeasurementSetPlot(
        store: WorkbenchStore,
        datasetID: String,
        timeoutSeconds: TimeInterval
    ) -> Bool {
        let deadline = Date().addingTimeInterval(timeoutSeconds)
        while Date() < deadline {
            let plotState = store.state.measurementSetPlots[datasetID]
            if plotState?.status == .ready && plotState?.result != nil {
                return true
            }
            if plotState?.status == .failed {
                return false
            }
            RunLoop.current.run(mode: .default, before: Date().addingTimeInterval(0.05))
        }
        return false
    }

    private func openStartupProjectIfNeeded() {
        guard !didOpenStartupProject else { return }
        didOpenStartupProject = true
        if let startupTutorialPackPath {
            store.openTutorialPack(path: startupTutorialPackPath)
            if let startupTutorialSectionID {
                store.selectTutorialSection(startupTutorialSectionID)
            }
        } else if let startupImagerMeasurementSetPath {
            store.openExternalMeasurementSetForDirtyImaging(path: startupImagerMeasurementSetPath)
        } else if let startupProjectPath {
            store.openProject(path: startupProjectPath)
        }
        if startupOpenSelectedDatasetExplorer || !startupImageRegionBoxes.isEmpty {
            store.openDefaultTab(kind: .datasetExplorer)
            Self.applyImageRegionBoxes(startupImageRegionBoxes, store: store)
            Self.exportImageRegionIfNeeded(startupImageRegionExportPath, store: store)
        }
        if startupShowImagerProgressMockup {
            store.openImagerProgressMockup()
        } else if startupOpenImagerTask && startupImagerMeasurementSetPath == nil {
            store.openDirtyImagingTaskForSelectedDataset()
            store.selectTask("imager")
            store.setGenericTaskConfirmation(taskID: "imager", confirmed: true)
            if startupRunActiveTask {
                store.runTask()
            }
        } else if startupRunActiveTask && startupImagerMeasurementSetPath != nil {
            store.selectTask("imager")
            store.setGenericTaskConfirmation(taskID: "imager", confirmed: true)
            store.runTask()
        }
    }

    private func syncStoreFontSizeFromSettings() {
        store.setInterfaceFontSize(interfaceFontSize)
    }

    private func setStoredInterfaceFontSize(_ size: Double) {
        interfaceFontSize = WorkbenchState.clampedInterfaceFontSize(size)
    }

    private static func storedInterfaceFontSize() -> Double {
        guard let value = UserDefaults.standard.object(forKey: Self.interfaceFontSizeKey) as? Double else {
            return WorkbenchState.defaultInterfaceFontSize
        }
        return WorkbenchState.clampedInterfaceFontSize(value)
    }
}

struct DisplaySettingsView: View {
    @Binding var interfaceFontSize: Double

    var body: some View {
        Form {
            Section("Display") {
                HStack {
                    Text("Font size")
                    Spacer()
                    Stepper(
                        value: $interfaceFontSize,
                        in: WorkbenchState.minimumInterfaceFontSize...WorkbenchState.maximumInterfaceFontSize,
                        step: 1
                    ) {
                        Text("\(Int(interfaceFontSize.rounded())) pt")
                            .monospacedDigit()
                            .frame(width: 48, alignment: .trailing)
                    }
                    .accessibilityIdentifier("settings.fontSize.stepper")
                }

                Slider(
                    value: $interfaceFontSize,
                    in: WorkbenchState.minimumInterfaceFontSize...WorkbenchState.maximumInterfaceFontSize,
                    step: 1
                )
                .accessibilityIdentifier("settings.fontSize.slider")

                Button("Reset Font Size") {
                    interfaceFontSize = WorkbenchState.defaultInterfaceFontSize
                }
                .accessibilityIdentifier("settings.fontSize.reset")
            }
        }
        .padding(24)
        .frame(width: 360)
    }
}

final class AppDelegate: NSObject, NSApplicationDelegate {
    func applicationDidFinishLaunching(_ notification: Notification) {
        NSApp.setActivationPolicy(.regular)
        NSApp.activate(ignoringOtherApps: true)
    }
}

enum FullScreenController {
    static func toggleFullScreen() {
        if let window = NSApp.keyWindow ?? NSApp.mainWindow ?? NSApp.windows.first(where: { $0.canBecomeKey }) {
            window.toggleFullScreen(nil)
        }
    }
}

enum ProjectOpenPanel {
    static func chooseDirectory() -> URL? {
        let panel = NSOpenPanel()
        panel.canChooseFiles = false
        panel.canChooseDirectories = true
        panel.treatsFilePackagesAsDirectories = true
        panel.allowsMultipleSelection = false
        panel.prompt = "Open"
        return panel.runModal() == .OK ? panel.url : nil
    }
}

enum TutorialPackOpenPanel {
    static func choosePack() -> URL? {
        let panel = NSOpenPanel()
        panel.canChooseFiles = true
        panel.canChooseDirectories = true
        panel.treatsFilePackagesAsDirectories = true
        panel.allowsMultipleSelection = false
        panel.prompt = "Open Tutorial Pack"
        return panel.runModal() == .OK ? panel.url : nil
    }
}

struct WindowConfigurationView: NSViewRepresentable {
    func makeNSView(context: Context) -> WindowConfigurationHost {
        WindowConfigurationHost()
    }

    func updateNSView(_ nsView: WindowConfigurationHost, context: Context) {
        nsView.configureWindowIfPossible()
    }
}

final class WindowConfigurationHost: NSView {
    override func viewDidMoveToWindow() {
        super.viewDidMoveToWindow()
        configureWindowIfPossible()
    }

    func configureWindowIfPossible() {
        guard let window else { return }

        window.title = "casa-rs Workbench"
        window.minSize = NSSize(width: 1120, height: 720)
        window.styleMask.insert([.titled, .closable, .miniaturizable, .resizable])
        window.collectionBehavior = [.managed, .fullScreenPrimary]
    }
}
