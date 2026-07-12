import Foundation

/// Deterministic Wave 4 review scenarios. These values are fixture-only and
/// never enter assistant provider, transcript, proposal, or corpus contracts.
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

package struct PrototypeAIProvider: Identifiable, Codable, Equatable {
    package let id: String
    package var label: String
    package var models: [String]
}

package struct PrototypeAIContext: Identifiable, Codable, Equatable {
    package let id: String
    package var label: String
    package var detail: String
    package var selected: Bool
    package var egressSummary: String
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
    package var providerLabel: String?
    package var modelLabel: String?
    package var citations: [PrototypeAICitation]
    package var usedContextIDs: [String]
    package var pinned: Bool
}

package enum PrototypeAIProposalKind: String, CaseIterable, Codable, Equatable {
    case task
    case python
    case plot
    case download
    case note

    package var label: String {
        switch self {
        case .task: "Task"
        case .python: "Python"
        case .plot: "Plot"
        case .download: "Download"
        case .note: "Note"
        }
    }
}

package enum PrototypeAIProposalState: String, Codable, Equatable {
    case pending
    case running
    case succeeded
    case failed
    case rejected
    case cancelled
}

package struct PrototypeAIProposal: Identifiable, Codable, Equatable {
    package let id: String
    package var kind: PrototypeAIProposalKind
    package var title: String
    package var summary: String
    package var exactPayload: String
    package var authority: String
    package var affectedPaths: [String]
    package var state: PrototypeAIProposalState
    package var result: String?
}

/// Mutable in-memory projection for the Wave 4 interaction prototype.
package struct PrototypeAIChatProjection: Codable, Equatable {
    package var prototypeKind: WorkbenchPrototypeKind = .ai
    package var scenario: AIChatPrototypeScenario
    package var providers: [PrototypeAIProvider]
    package var selectedProviderID: String
    package var selectedModel: String
    package var contexts: [PrototypeAIContext]
    package var corpusState: PrototypeAIActivityState
    package var responseState: PrototypeAIActivityState
    package var messages: [PrototypeAIMessage]
    package var proposals: [PrototypeAIProposal]
    package var generation: Int
    package var activePrompt: String?
    package var failureConsumed: Bool
    package var pinnedMessageCount: Int
    package var insertedPlotCount: Int
    package var productionBoundaryCalls: Int

    package var selectedProvider: PrototypeAIProvider? {
        providers.first { $0.id == selectedProviderID }
    }

    package var selectedContexts: [PrototypeAIContext] {
        contexts.filter(\.selected)
    }

    package mutating func selectProvider(_ id: String) {
        guard let provider = providers.first(where: { $0.id == id }) else { return }
        selectedProviderID = provider.id
        selectedModel = provider.models.first ?? "Fixture model"
    }

    package mutating func selectModel(_ model: String) {
        guard selectedProvider?.models.contains(model) == true else { return }
        selectedModel = model
    }

    package mutating func toggleContext(_ id: String) {
        guard let index = contexts.firstIndex(where: { $0.id == id }) else { return }
        contexts[index].selected.toggle()
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
            providerLabel: nil,
            modelLabel: nil,
            citations: [],
            usedContextIDs: [],
            pinned: false
        ))
        return generation
    }

    package mutating func completeResponse(generation: Int) {
        guard generation == self.generation, responseState == .streaming else { return }
        if !failureConsumed {
            switch scenario {
            case .providerError:
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
            case .primary, .toolFailure:
                break
            }
        }
        let provider = selectedProvider
        let citations = PrototypeAIChatFixtureAdapter.answerCitations
        messages.append(PrototypeAIMessage(
            id: "ai-assistant-\(generation)",
            role: .assistant,
            text: "TW Hya's compact continuum emission is consistent with a nearly face-on disk. CASA-RS currently builds this view through the Rust-owned MeasurementSet plot-data path, then projects it to Swift.",
            providerLabel: provider?.label,
            modelLabel: selectedModel,
            citations: citations,
            usedContextIDs: selectedContexts.map(\.id),
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

    package mutating func pinMessage(_ id: String) {
        guard let index = messages.firstIndex(where: { $0.id == id }), !messages[index].pinned else { return }
        messages[index].pinned = true
        pinnedMessageCount += 1
    }

    package mutating func rejectProposal(_ id: String) {
        guard let index = proposals.firstIndex(where: { $0.id == id }), proposals[index].state == .pending else { return }
        proposals[index].state = .rejected
        proposals[index].result = "Rejected; no fixture action was invoked."
    }

    package mutating func beginProposal(_ id: String) -> Int? {
        guard let index = proposals.firstIndex(where: { $0.id == id }), proposals[index].state == .pending else { return nil }
        generation += 1
        proposals[index].state = .running
        return generation
    }

    package mutating func completeProposal(_ id: String, generation: Int) {
        guard generation == self.generation,
              let index = proposals.firstIndex(where: { $0.id == id }),
              proposals[index].state == .running
        else { return }
        if scenario == .toolFailure && proposals[index].kind == .task && !failureConsumed {
            failureConsumed = true
            proposals[index].state = .failed
            proposals[index].result = "Fixture task validation failed before invocation."
            return
        }
        proposals[index].state = .succeeded
        proposals[index].result = switch proposals[index].kind {
        case .task: "Fixture task proposal completed; no task provider was invoked."
        case .python: "Fixture calculation produced one staged result."
        case .plot: "Fixture plot revision inserted into the notebook preview."
        case .download: "Fixture download plan completed without network access."
        case .note: "Fixture note pinned into the notebook preview."
        }
        if proposals[index].kind == .plot { insertedPlotCount += 1 }
    }

    package mutating func cancelProposal(_ id: String) {
        guard let index = proposals.firstIndex(where: { $0.id == id }), proposals[index].state == .running else { return }
        generation += 1
        proposals[index].state = .cancelled
        proposals[index].result = "Cancelled; authority was not broadened or retried."
    }

    package mutating func retryProposal(_ id: String) {
        guard let index = proposals.firstIndex(where: { $0.id == id }),
              proposals[index].state == .failed || proposals[index].state == .cancelled
        else { return }
        proposals[index].state = .pending
        proposals[index].result = nil
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
            providers: [
                PrototypeAIProvider(id: "fixture-openai", label: "OpenAI subscription", models: ["GPT-5", "GPT-5 mini"]),
                PrototypeAIProvider(id: "fixture-zen", label: "OpenCode Zen", models: ["Qwen3 Coder", "GLM-4.5"]),
            ],
            selectedProviderID: "fixture-openai",
            selectedModel: "GPT-5",
            contexts: [
                PrototypeAIContext(id: "project", label: "Project", detail: "Dataset metadata and bounded summaries", selected: true, egressSummary: "2 metadata summaries; raw visibilities excluded"),
                PrototypeAIContext(id: "paper", label: "TW Hya paper", detail: "Copied project paper with page citations", selected: true, egressSummary: "3 cited excerpts; author email redacted"),
                PrototypeAIContext(id: "source", label: "CASA-RS source", detail: "Commit-keyed local source overlay", selected: true, egressSummary: "2 source excerpts with path and symbol"),
                PrototypeAIContext(id: "plot", label: "Current plot", detail: "Downsampled plot summary and preview", selected: false, egressSummary: "1 bounded plot summary; arrays excluded"),
            ],
            corpusState: scenario == .offline ? .offline : .ready,
            responseState: .completed,
            messages: [
                PrototypeAIMessage(id: "ai-user-initial", role: .user, text: "What does the continuum plot suggest, and how is it implemented?", providerLabel: nil, modelLabel: nil, citations: [], usedContextIDs: [], pinned: false),
                PrototypeAIMessage(id: "ai-assistant-initial", role: .assistant, text: "The compact emission is consistent with a nearly face-on disk. The plot is built from Rust-owned MeasurementSet series and projected to the native GUI.", providerLabel: "OpenAI subscription", modelLabel: "GPT-5", citations: answerCitations, usedContextIDs: ["project", "paper", "source"], pinned: false),
            ],
            proposals: proposalFixtures(),
            generation: 0,
            activePrompt: nil,
            failureConsumed: false,
            pinnedMessageCount: 0,
            insertedPlotCount: 0,
            productionBoundaryCalls: 0
        )
    }

    private static func proposalFixtures() -> [PrototypeAIProposal] {
        [
            PrototypeAIProposal(id: "proposal-task", kind: .task, title: "Run a safer continuum image", summary: "Open the canonical Imager task with a sparse typed proposal.", exactPayload: "vis = data/twhya_calibrated.ms\nimagename = products/twhya_robust\nrobust = -0.5", authority: "Task execution · explicit approval", affectedPaths: ["products/twhya_robust.*"], state: .pending, result: nil),
            PrototypeAIProposal(id: "proposal-python", kind: .python, title: "Calculate disk inclination", summary: "Run exact visible code in the restricted AI worker.", exactPayload: "inclination = acos(minor_axis / major_axis)\nprint(degrees(inclination))", authority: "AI Python · no network · staged artifacts only", affectedPaths: ["notebooks/assets/ai/inclination.txt"], state: .pending, result: nil),
            PrototypeAIProposal(id: "proposal-plot", kind: .plot, title: "Plot deprojected amplitude", summary: "Generate a scientific figure and insert an immutable notebook revision.", exactPayload: "plot_deprojected_amplitude(ms='data/twhya_calibrated.ms', bins=40)", authority: "AI Python plot · explicit notebook insertion", affectedPaths: ["notebooks/assets/ai/deprojected-amplitude.png"], state: .pending, result: nil),
            PrototypeAIProposal(id: "proposal-download", kind: .download, title: "Download a comparison paper", summary: "Review source, size, destination, and checksum before acquisition.", exactPayload: "https://example.invalid/twhya-comparison.pdf", authority: "Public download · project documents only", affectedPaths: ["documents/twhya-comparison.pdf"], state: .pending, result: nil),
            PrototypeAIProposal(id: "proposal-note", kind: .note, title: "Pin conclusion to notebook", summary: "Insert only the selected visible conclusion and citations.", exactPayload: "The compact continuum is consistent with a nearly face-on disk. [Andrews et al. 2016, p.4]", authority: "Notebook note write · exact visible text", affectedPaths: ["notebooks/default.md"], state: .pending, result: nil),
        ]
    }
}
