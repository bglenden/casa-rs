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
    @StateObject private var store = WorkbenchStore.fixture()

    init() {
        let arguments = CommandLine.arguments
        if arguments.contains("--dump-debug-state") {
            dumpDebugState(
                simulateMainFlow: arguments.contains("--simulate-main-flow"),
                projectPath: argumentValue(after: "--probe-project", in: arguments)
            )
            exit(0)
        }
    }

    var body: some Scene {
        WindowGroup("casa-rs Workbench") {
            WorkbenchView(store: store)
                .frame(minWidth: 1120, minHeight: 720)
                .environment(\.workbenchFontSize, store.state.interfaceFontSize)
                .background(WindowConfigurationView())
                .onAppear {
                    syncStoreFontSizeFromSettings()
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
                Button("Open Fixture Project") {
                    store.openFixtureProject()
                }
                .keyboardShortcut("o", modifiers: [.command])

                Button("Open Project Directory...") {
                    if let url = ProjectOpenPanel.chooseDirectory() {
                        store.openProject(path: url.path)
                    }
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

    private func dumpDebugState(simulateMainFlow: Bool, projectPath: String?) {
        let store = WorkbenchStore.fixture()
        store.setInterfaceFontSize(storedInterfaceFontSize())
        if let projectPath {
            store.openProject(path: projectPath)
        }
        if simulateMainFlow {
            store.selectDockMode(.history)
            store.setInspectorCollapsed(true)
            store.applyAIProposal("proposal-spw")
            store.setPythonOwner(.ai)
            store.runTask()
            store.openDefaultTab(kind: .history)
        }

        do {
            let json = try store.debugJSON()
            print(json)
        } catch {
            fputs("failed to encode debug state: \(error)\n", stderr)
            exit(1)
        }
    }

    private func argumentValue(after flag: String, in arguments: [String]) -> String? {
        guard let index = arguments.firstIndex(of: flag), arguments.indices.contains(index + 1) else {
            return nil
        }
        return arguments[index + 1]
    }

    private func syncStoreFontSizeFromSettings() {
        store.setInterfaceFontSize(interfaceFontSize)
    }

    private func setStoredInterfaceFontSize(_ size: Double) {
        interfaceFontSize = WorkbenchState.clampedInterfaceFontSize(size)
    }

    private func storedInterfaceFontSize() -> Double {
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
        panel.allowsMultipleSelection = false
        panel.prompt = "Open"
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
