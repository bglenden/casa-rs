import CasarsMacCore
import Darwin
import Foundation
import SwiftUI

@main
struct CasarsMacApp: App {
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
