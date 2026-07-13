import Foundation

/// Deterministic Wave 4 review scenarios. These values are fixture-only and
/// never enter agent, transcript, authority, or corpus contracts.
package enum AIChatPrototypeScenario: String, Codable, Equatable {
    case primary = "happy-path"
    case providerError = "provider-error"
    case rateLimited = "rate-limited"
    case offline
    case toolFailure = "tool-failure"
    case nonresponsive
}

package enum PrototypeAIActivityState: String, Codable, Equatable {
    case idle
    case indexing
    case ready
    case streaming
    case completed
    case rateLimited = "rate-limited"
    case offline
    case failed
    case cancelled
    case restartRequired = "restart-required"
}

package enum PrototypeAIChatPresentation: String, Codable, Equatable {
    case closed
    case drawer
    case tab
}

package enum PrototypeAITrustPreset: String, CaseIterable, Codable, Equatable {
    case explore
    case work
    case fullAccess = "full-access"

    package var label: String {
        switch self {
        case .explore: "Explore"
        case .work: "Work"
        case .fullAccess: "Full access"
        }
    }

    package var detail: String {
        switch self {
        case .explore:
            "CASA context only; no project instructions, shell, writes, or network"
        case .work:
            "Trusted project, user shell/Python, and native Codex approvals"
        case .fullAccess:
            "Unrestricted expert mode; explicitly confirmed and always visible"
        }
    }
}

package struct PrototypeAIWorkspaceSource: Identifiable, Codable, Equatable {
    package let id: String
    package var label: String
    package var detail: String
    package var openTab: Bool
}

package struct PrototypeAIAgent: Identifiable, Codable, Equatable {
    package let id: String
    package var label: String
    package var detail: String
    package var models: [String]
    package var enabled: Bool
}

package struct PrototypeAIAccount: Codable, Equatable {
    package var label: String
    package var status: String
    package var funding: String
}

package enum PrototypeAIReasoningEffort: String, CaseIterable, Codable, Equatable, Identifiable {
    case low
    case medium
    case high

    package var id: String { rawValue }

    package var label: String {
        rawValue.capitalized
    }
}

package struct PrototypeAIUsage: Codable, Equatable {
    package var fiveHourRemainingPercent: Int
    package var weeklyRemainingPercent: Int
    package var fiveHourReset: String
    package var weeklyReset: String

    package var compactLabel: String {
        "5h \(fiveHourRemainingPercent)% · week \(weeklyRemainingPercent)%"
    }
}

package struct PrototypeAIPythonEnvironment: Identifiable, Codable, Equatable {
    package let id: String
    package var label: String
    package var detail: String
}

package struct PrototypeAIContext: Identifiable, Codable, Equatable {
    package let id: String
    package var label: String
    package var detail: String
}

package struct PrototypeAICitation: Identifiable, Codable, Equatable {
    package let id: String
    package var label: String
    package var locator: String
    package var excerpt: String
}

package enum PrototypeAIMessageRole: String, Codable, Equatable {
    case user
    case assistant
}

package struct PrototypeAIMessage: Identifiable, Codable, Equatable {
    package let id: String
    package var role: PrototypeAIMessageRole
    package var text: String
    package var agentLabel: String?
    package var modelLabel: String?
    package var citations: [PrototypeAICitation]
    package var usedContextIDs: [String]
    package var activity: [String]
    package var suggestedTaskID: String?
    package var pinned: Bool
}

/// Mutable in-memory projection for the Wave 4 interaction prototype.
package struct PrototypeAIChatProjection: Codable, Equatable {
    package var prototypeKind: WorkbenchPrototypeKind = .ai
    package var scenario: AIChatPrototypeScenario
    package var presentation: PrototypeAIChatPresentation
    package var primaryAttachment: String
    package var draft: String
    package var suggestedPrompts: [String]
    package var workspaceSources: [PrototypeAIWorkspaceSource]
    package var agents: [PrototypeAIAgent]
    package var selectedAgentID: String
    package var selectedModel: String
    package var reasoningEffort: PrototypeAIReasoningEffort
    package var account: PrototypeAIAccount
    package var usage: PrototypeAIUsage
    package var trustPreset: PrototypeAITrustPreset
    package var pythonEnvironments: [PrototypeAIPythonEnvironment]
    package var selectedPythonEnvironmentID: String
    package var contexts: [PrototypeAIContext]
    package var corpusState: PrototypeAIActivityState
    package var responseState: PrototypeAIActivityState
    package var messages: [PrototypeAIMessage]
    package var generation: Int
    package var activePrompt: String?
    package var failureConsumed: Bool
    package var pinnedMessageCount: Int
    package var productionBoundaryCalls: Int
    package var notebookPinFocusGeneration: Int

    package var openTabSources: [PrototypeAIWorkspaceSource] {
        workspaceSources.filter(\.openTab)
    }

    package var selectedAgent: PrototypeAIAgent? {
        agents.first { $0.id == selectedAgentID }
    }

    package var selectedPythonEnvironment: PrototypeAIPythonEnvironment? {
        pythonEnvironments.first { $0.id == selectedPythonEnvironmentID }
    }

    package mutating func selectAgent(_ id: String) {
        guard let agent = agents.first(where: { $0.id == id }), agent.enabled else { return }
        selectedAgentID = agent.id
        selectedModel = agent.models.first ?? "Fixture model"
    }

    package mutating func selectModel(_ model: String) {
        guard selectedAgent?.models.contains(model) == true else { return }
        selectedModel = model
    }

    package mutating func selectReasoningEffort(_ effort: PrototypeAIReasoningEffort) {
        reasoningEffort = effort
    }

    package mutating func selectTrustPreset(_ preset: PrototypeAITrustPreset) {
        trustPreset = preset
    }

    package mutating func selectPythonEnvironment(_ id: String) {
        guard pythonEnvironments.contains(where: { $0.id == id }) else { return }
        selectedPythonEnvironmentID = id
    }

    package mutating func setPresentation(_ presentation: PrototypeAIChatPresentation) {
        self.presentation = presentation
    }

    package mutating func setDraft(_ draft: String) {
        self.draft = draft
    }

    package mutating func requestNotebookPinFocus() {
        notebookPinFocusGeneration += 1
    }

    package mutating func beginIndexing() -> Int {
        generation += 1
        corpusState = .indexing
        return generation
    }

    package mutating func completeIndexing(generation: Int) {
        guard generation == self.generation, corpusState == .indexing else { return }
        if scenario == .offline && !failureConsumed {
            failureConsumed = true
            corpusState = .offline
        } else {
            corpusState = .ready
        }
    }

    package mutating func cancelIndexing() {
        guard corpusState == .indexing else { return }
        generation += 1
        corpusState = .cancelled
    }

    package mutating func beginResponse(prompt: String) -> Int? {
        guard corpusState == .ready, responseState != .streaming else { return nil }
        let normalized = prompt.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !normalized.isEmpty else { return nil }
        generation += 1
        activePrompt = normalized
        responseState = .streaming
        messages.append(PrototypeAIMessage(
            id: "ai-user-\(generation)",
            role: .user,
            text: normalized,
            agentLabel: nil,
            modelLabel: nil,
            citations: [],
            usedContextIDs: [],
            activity: [],
            suggestedTaskID: nil,
            pinned: false
        ))
        return generation
    }

    package mutating func completeResponse(generation: Int) {
        guard generation == self.generation, responseState == .streaming else { return }
        if !failureConsumed {
            switch scenario {
            case .providerError, .toolFailure:
                failureConsumed = true
                responseState = .failed
                return
            case .rateLimited:
                failureConsumed = true
                responseState = .rateLimited
                return
            case .offline:
                failureConsumed = true
                responseState = .offline
                return
            case .nonresponsive:
                return
            case .primary:
                break
            }
        }
        messages.append(PrototypeAIMessage(
            id: "ai-assistant-\(generation)",
            role: .assistant,
            text: "TW Hya's compact continuum emission is consistent with a nearly face-on disk. CASA-RS currently builds this view through the Rust-owned MeasurementSet plot-data path, then projects it to Swift.",
            agentLabel: selectedAgent?.label,
            modelLabel: selectedModel,
            citations: PrototypeAIChatFixtureAdapter.answerCitations,
            usedContextIDs: contexts.map(\.id),
            activity: [
                "Read the active Analysis.md section and open Imager parameters",
                "Retrieved one project-paper page and the matching CASA-RS symbol",
                "Compared plot metadata without loading raw visibility arrays",
            ],
            suggestedTaskID: "imager",
            pinned: false
        ))
        activePrompt = nil
        responseState = .completed
    }

    package mutating func cancelResponse() {
        guard responseState == .streaming else { return }
        generation += 1
        responseState = scenario == .nonresponsive ? .restartRequired : .cancelled
    }

    package mutating func restartResponse() {
        generation += 1
        responseState = .idle
        activePrompt = nil
        failureConsumed = true
    }

    package mutating func pinMessage(_ id: String) -> PrototypeAIMessage? {
        guard let index = messages.firstIndex(where: { $0.id == id }), !messages[index].pinned else {
            return nil
        }
        messages[index].pinned = true
        pinnedMessageCount += 1
        requestNotebookPinFocus()
        return messages[index]
    }
}

package enum PrototypeAIChatFixtureAdapter {
    package static let answerCitations = [
        PrototypeAICitation(
            id: "citation-paper",
            label: "Andrews et al. 2016",
            locator: "TW Hya disk paper · page 4",
            excerpt: "The continuum morphology is compact and close to face-on at the angular scales used here."
        ),
        PrototypeAICitation(
            id: "citation-source",
            label: "casa-ms source",
            locator: "crates/casa-ms/src/msexplore.rs · build_plot_document",
            excerpt: "Rust constructs the renderer-neutral plot series and point provenance before frontend projection."
        ),
    ]

    package static func make(scenario: AIChatPrototypeScenario) -> PrototypeAIChatProjection {
        PrototypeAIChatProjection(
            scenario: scenario,
            presentation: .closed,
            primaryAttachment: "notebooks/Analysis.md",
            draft: "",
            suggestedPrompts: [
                "Compare the current plot with the TW Hya paper.",
                "Explain how the open Imager task parameters affect this result.",
                "How does CASA-RS represent this MeasurementSet on disk?",
            ],
            workspaceSources: [
                PrototypeAIWorkspaceSource(id: "tab-notebook", label: "Analysis.md", detail: "Complete notebook, current section, and selection", openTab: true),
                PrototypeAIWorkspaceSource(id: "tab-task", label: "Imager task", detail: "Schema, current values, defaults, and non-default parameters", openTab: true),
                PrototypeAIWorkspaceSource(id: "tab-explorer", label: "TW Hya explorer", detail: "Selected dataset, plot configuration, and bounded preview", openTab: true),
                PrototypeAIWorkspaceSource(id: "tab-python", label: "Python output 17", detail: "Cell source, selected environment, result, and figure", openTab: true),
                PrototypeAIWorkspaceSource(id: "tab-history", label: "Processing history", detail: "Receipts, products, statuses, and diagnostics", openTab: true),
                PrototypeAIWorkspaceSource(id: "corpus-radio", label: "Radio astronomy corpus", detail: "4,812 fixture documents with page/section retrieval", openTab: false),
                PrototypeAIWorkspaceSource(id: "corpus-project", label: "Project papers", detail: "2 user-supplied papers plus notebook references", openTab: false),
                PrototypeAIWorkspaceSource(id: "source-casars", label: "CASA-RS source", detail: "Release source plus live-checkout overlay at fixture commit 597da3f", openTab: false),
                PrototypeAIWorkspaceSource(id: "schema-casars", label: "CASA-RS semantics", detail: "Tasks, parameters, tables, MeasurementSets, images, coordinates, and measures", openTab: false),
            ],
            agents: [
                PrototypeAIAgent(id: "codex-app-server", label: "Codex", detail: "Direct App Server · initial Wave 4 target", models: ["gpt-5.4", "gpt-5.3-codex"], enabled: true),
                PrototypeAIAgent(id: "opencode-acp", label: "OpenCode", detail: "Future ACP adapter", models: [], enabled: false),
            ],
            selectedAgentID: "codex-app-server",
            selectedModel: "gpt-5.4",
            reasoningEffort: .medium,
            account: PrototypeAIAccount(label: "ChatGPT Pro", status: "Connected through Codex · fixture", funding: "Subscription · no API billing"),
            usage: PrototypeAIUsage(
                fiveHourRemainingPercent: 72,
                weeklyRemainingPercent: 44,
                fiveHourReset: "Resets in 2 h 18 m · fixture",
                weeklyReset: "Resets Monday 6:00 PM · fixture"
            ),
            trustPreset: .work,
            pythonEnvironments: [
                PrototypeAIPythonEnvironment(id: "casa-python", label: "CASA 6.7 Python", detail: "~/SoftwareProjects/casa-build/venv/bin/python · fixture"),
                PrototypeAIPythonEnvironment(id: "login-python", label: "Login-shell Python", detail: "/usr/local/bin/python3 · fixture"),
            ],
            selectedPythonEnvironmentID: "casa-python",
            contexts: [
                PrototypeAIContext(id: "project", label: "Open project tabs", detail: "5 typed semantic projections available through CASA MCP"),
                PrototypeAIContext(id: "paper", label: "Radio astronomy + project papers", detail: "Local FTS retrieval with page/section citations"),
                PrototypeAIContext(id: "source", label: "CASA-RS implementation", detail: "Release/live source with path, symbol, lines, and commit"),
                PrototypeAIContext(id: "semantics", label: "CASA task and data semantics", detail: "Schemas, defaults, receipts, persistent types, and canonical actions"),
            ],
            corpusState: scenario == .offline ? .offline : .ready,
            responseState: .idle,
            messages: [],
            generation: 0,
            activePrompt: nil,
            failureConsumed: false,
            pinnedMessageCount: 0,
            productionBoundaryCalls: 0,
            notebookPinFocusGeneration: 0
        )
    }
}
