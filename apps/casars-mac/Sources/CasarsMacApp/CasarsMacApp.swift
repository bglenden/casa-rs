import CasarsMacCore
import AppKit
import Darwin
import Foundation
import SwiftUI

@main
struct CasarsMacApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) private var appDelegate
    @StateObject private var store = WorkbenchStore.fixture()

    init() {
        let arguments = CommandLine.arguments
        if arguments.contains("--dump-debug-state") {
            dumpDebugState(simulateMainFlow: arguments.contains("--simulate-main-flow"))
            exit(0)
        }
    }

    var body: some Scene {
        WindowGroup("casa-rs Workbench") {
            WorkbenchView(store: store)
                .frame(minWidth: 1120, minHeight: 720)
                .background(WindowConfigurationView())
        }
        .commands {
            CommandMenu("Workbench") {
                Button("Open Fixture Project") {
                    store.openFixtureProject()
                }
                .keyboardShortcut("o", modifiers: [.command])

                Button("Open AI Chat") {
                    store.openDefaultTab(kind: .aiChat)
                }
                .keyboardShortcut("l", modifiers: [.command, .shift])

                Button("Toggle Inspector") {
                    store.toggleInspector()
                }
                .keyboardShortcut("i", modifiers: [.command, .option])

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
        }
    }

    private func dumpDebugState(simulateMainFlow: Bool) {
        let store = WorkbenchStore.fixture()
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
