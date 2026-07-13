import AppKit
import CasarsMacCore
import SwiftUI

private let aiPrototypePurple = Color.purple

enum AIChatPrototypeLayout {
    case drawer
    case expanded
}

struct AIChatPrototypeView: View {
    @ObservedObject var store: WorkbenchStore
    let layout: AIChatPrototypeLayout

    @State private var selectedCitationID: String?
    @State private var sourceCitation: PrototypeAICitation?
    @State private var contextOpen = false
    @State private var confirmingFullAccess = false
    @State private var settingsOpen = false
    @State private var usageOpen = false

    private var projection: PrototypeAIChatProjection? {
        store.state.prototypeAI
    }

    var body: some View {
        ZStack(alignment: .bottomTrailing) {
            VStack(spacing: 0) {
                if let projection {
                    header(projection)
                    Divider()
                    conversation(projection)
                    Divider()
                    composer(projection)
                } else {
                    Text("AI prototype fixture unavailable")
                        .foregroundStyle(.secondary)
                }
            }

            if contextOpen, let projection {
                contextPanel(projection)
                    .padding(.trailing, 10)
                    .padding(.bottom, 154)
            }
        }
        .background(Color(nsColor: .textBackgroundColor))
        .sheet(item: $sourceCitation) { citation in
            sourceSheet(citation)
        }
        .sheet(isPresented: $confirmingFullAccess) {
            fullAccessSheet
        }
    }

    private func header(_ projection: PrototypeAIChatProjection) -> some View {
        VStack(alignment: .leading, spacing: 7) {
            HStack(spacing: 8) {
                Image(systemName: "sparkles")
                    .foregroundStyle(aiPrototypePurple)

                VStack(alignment: .leading, spacing: 1) {
                    Text(layout == .drawer ? "Notebook chat" : "AI · TW Hya discussion")
                        .workbenchFont(.headline)
                        .accessibilityIdentifier(
                            layout == .drawer ? "aiPrototype.drawer" : "aiPrototype.expanded"
                        )
                    Text("Attached to \(projection.primaryAttachment)")
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)
                        .lineLimit(1)
                        .accessibilityIdentifier("aiPrototype.attachment")
                }

                Spacer()

                Menu {
                    Button("TW Hya continuum", systemImage: "checkmark") {}
                    Button("Imaging strategy") {}
                    Divider()
                    Button("New fixture discussion", systemImage: "plus") {
                        store.setAIPrototypeDraft("")
                    }
                } label: {
                    Image(systemName: "clock")
                }
                .menuStyle(.borderlessButton)
                .help("Conversation history")
                .accessibilityLabel("Conversation history")
                .accessibilityIdentifier("aiPrototype.history")

                if layout == .drawer {
                    Button {
                        store.expandAIPrototypeConversation()
                    } label: {
                        Image(systemName: "arrow.up.left.and.arrow.down.right")
                    }
                    .buttonStyle(.borderless)
                    .foregroundStyle(aiPrototypePurple)
                    .help("Open in AI tab")
                    .accessibilityLabel("Open in AI tab")
                    .accessibilityIdentifier("aiPrototype.expand")

                    Button {
                        store.closeAIPrototypeConversation()
                    } label: {
                        Image(systemName: "xmark")
                    }
                    .buttonStyle(.borderless)
                    .help("Close chat")
                    .accessibilityLabel("Close chat")
                    .accessibilityIdentifier("aiPrototype.closeDrawer")
                } else {
                    Button("Dock beside notebook") {
                        store.dockAIPrototypeConversation()
                    }
                    .controlSize(.small)
                    .tint(aiPrototypePurple)
                    .accessibilityIdentifier("aiPrototype.dock")
                }
            }

            HStack(spacing: 8) {
                if projection.trustPreset == .fullAccess {
                    Label("Full access", systemImage: "exclamationmark.shield.fill")
                        .workbenchFont(.caption, weight: .semibold)
                        .foregroundStyle(.orange)
                        .accessibilityIdentifier("aiPrototype.fullAccessIndicator")
                }
                Spacer()
                Label(
                    "Fixture · \(store.prototypeProductionBoundaryInvocationCount) calls",
                    systemImage: "shippingbox"
                )
                .workbenchFont(.caption2)
                .foregroundStyle(.secondary)
                .accessibilityElement(children: .ignore)
                .accessibilityLabel(
                    "Fixture · \(store.prototypeProductionBoundaryInvocationCount) production calls"
                )
                .accessibilityValue("\(store.prototypeProductionBoundaryInvocationCount)")
                .accessibilityIdentifier("aiPrototype.boundaryStatus")
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 10)
        .background(.bar)
    }

    private func conversation(_ projection: PrototypeAIChatProjection) -> some View {
        ScrollView {
            LazyVStack(alignment: .leading, spacing: 12) {
                if projection.messages.isEmpty {
                    emptyConversation(projection)
                } else {
                    ForEach(projection.messages) { message in
                        messageRow(message, projection: projection)
                    }
                }

                if projection.responseState == .streaming {
                    HStack(spacing: 8) {
                        ProgressView().controlSize(.small)
                        Text("Codex is using CASA context…")
                            .foregroundStyle(.secondary)
                    }
                    .accessibilityIdentifier("aiPrototype.response.streaming")
                }

                responseNotice(projection)
            }
            .frame(maxWidth: layout == .expanded ? 760 : .infinity, alignment: .leading)
            .padding(layout == .expanded ? 24 : 12)
            .frame(maxWidth: .infinity)
        }
        .accessibilityIdentifier("aiPrototype.conversationScroll")
    }

    private func emptyConversation(_ projection: PrototypeAIChatProjection) -> some View {
        VStack(alignment: .leading, spacing: 12) {
            Label("Ask about this project", systemImage: "bubble.left.and.text.bubble.right")
                .workbenchFont(.title3, weight: .semibold)
            Text("Codex can query every open tab, the radio astronomy and project-paper corpus, CASA task semantics, and the current CASA-RS source through the trusted project MCP fixture.")
                .foregroundStyle(.secondary)

            Text("Try asking")
                .workbenchFont(.caption, weight: .semibold)
                .foregroundStyle(.secondary)

            ForEach(projection.suggestedPrompts, id: \.self) { prompt in
                Button(prompt) {
                    store.setAIPrototypeDraft(prompt)
                }
                .buttonStyle(.link)
                .tint(aiPrototypePurple)
                .multilineTextAlignment(.leading)
                .accessibilityIdentifier("aiPrototype.suggestion.\(suggestionID(prompt))")
            }
        }
        .padding(14)
        .background(aiPrototypePurple.opacity(0.07))
        .clipShape(RoundedRectangle(cornerRadius: 10))
    }

    private func messageRow(
        _ message: PrototypeAIMessage,
        projection: PrototypeAIChatProjection
    ) -> some View {
        HStack {
            if message.role == .user { Spacer(minLength: layout == .expanded ? 120 : 28) }

            VStack(alignment: .leading, spacing: 8) {
                HStack {
                    Text(message.role == .user ? "You" : "Codex")
                        .workbenchFont(.caption, weight: .semibold)
                        .foregroundStyle(.secondary)
                        .accessibilityIdentifier("aiPrototype.message.\(message.id)")
                    if let agent = message.agentLabel, let model = message.modelLabel {
                        Text("\(agent) · \(model)")
                            .workbenchFont(.caption2)
                            .foregroundStyle(.tertiary)
                            .lineLimit(1)
                    }
                    Spacer()
                }

                Text(message.text)
                    .textSelection(.enabled)

                if !message.citations.isEmpty {
                    HStack(spacing: 6) {
                        ForEach(Array(message.citations.enumerated()), id: \.element.id) { index, citation in
                            Button("[\(index + 1)] \(citation.label)") {
                                selectedCitationID = selectedCitationID == citation.id ? nil : citation.id
                            }
                            .buttonStyle(.bordered)
                            .controlSize(.small)
                            .accessibilityIdentifier("aiPrototype.citation.\(citation.id)")
                        }
                    }
                }

                if let citation = selectedCitation(in: message) {
                    VStack(alignment: .leading, spacing: 4) {
                        Text(citation.locator)
                            .workbenchFont(.caption, weight: .semibold)
                        Text(citation.excerpt)
                            .workbenchFont(.caption)
                        Button("Open source preview") {
                            sourceCitation = citation
                        }
                        .buttonStyle(.link)
                        .accessibilityIdentifier("aiPrototype.citation.openSource")
                    }
                    .padding(8)
                    .background(aiPrototypePurple.opacity(0.09))
                    .clipShape(RoundedRectangle(cornerRadius: 7))
                    .accessibilityIdentifier("aiPrototype.sourcePreview")
                }

                if !message.activity.isEmpty {
                    DisclosureGroup("Agent activity · \(message.activity.count) steps") {
                        VStack(alignment: .leading, spacing: 5) {
                            ForEach(Array(message.activity.enumerated()), id: \.offset) { index, activity in
                                Text("\(index + 1). \(activity)")
                                    .workbenchFont(.caption)
                                    .foregroundStyle(.secondary)
                            }
                            Text("Used: \(contextLabels(message.usedContextIDs, projection: projection))")
                                .workbenchFont(.caption2)
                                .foregroundStyle(.tertiary)
                        }
                        .padding(.top, 4)
                    }
                    .workbenchFont(.caption)
                    .accessibilityIdentifier("aiPrototype.message.\(message.id).activity")
                }

                if message.role == .assistant {
                    HStack(spacing: 10) {
                        Button(message.pinned ? "Added to Analysis.md" : "Add to notebook") {
                            store.pinAIPrototypeMessage(message.id)
                        }
                        .buttonStyle(.borderedProminent)
                        .tint(aiPrototypePurple)
                        .controlSize(.small)
                        .disabled(message.pinned)
                        .accessibilityIdentifier("aiPrototype.message.\(message.id).addToNotebook")

                        if message.suggestedTaskID == "imager" {
                            Button("Open Imager task") {
                                store.openAIPrototypeTaskSuggestion()
                            }
                            .controlSize(.small)
                            .accessibilityIdentifier("aiPrototype.message.\(message.id).openTask")
                        }
                    }
                }
            }
            .padding(11)
            .background(
                message.role == .user
                    ? aiPrototypePurple.opacity(0.13)
                    : Color.secondary.opacity(0.08)
            )
            .clipShape(RoundedRectangle(cornerRadius: 10))

            if message.role == .assistant { Spacer(minLength: layout == .expanded ? 70 : 0) }
        }
    }

    @ViewBuilder
    private func responseNotice(_ projection: PrototypeAIChatProjection) -> some View {
        switch projection.responseState {
        case .rateLimited, .offline, .failed:
            HStack {
                Label(responseFailureLabel(projection.responseState), systemImage: "exclamationmark.triangle")
                    .foregroundStyle(.orange)
                    .accessibilityIdentifier("aiPrototype.response.error")
                Spacer()
                Button("Retry") { store.retryAIPrototypeResponse() }
                    .accessibilityIdentifier("aiPrototype.response.retry")
            }
            .padding(9)
            .background(Color.orange.opacity(0.08))
            .clipShape(RoundedRectangle(cornerRadius: 7))
        case .cancelled:
            Text("Response cancelled. It was not retried automatically.")
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
        case .restartRequired:
            HStack {
                Text("Agent process did not respond; restart is explicit.")
                    .accessibilityIdentifier("aiPrototype.response.restartRequired")
                Spacer()
                Button("Restart") { store.restartAIPrototypeWorker() }
                    .accessibilityIdentifier("aiPrototype.response.restart")
            }
            .foregroundStyle(.orange)
        default:
            EmptyView()
        }
    }

    private func composer(_ projection: PrototypeAIChatProjection) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            contextDisclosure(projection)

            HStack(alignment: .bottom, spacing: 8) {
                ZStack(alignment: .topLeading) {
                    if projection.draft.isEmpty {
                        Text("Ask anything about this project…")
                            .foregroundStyle(.tertiary)
                            .padding(.horizontal, 6)
                            .padding(.vertical, 8)
                    }
                    AIComposerEditor(
                        text: Binding(
                            get: { projection.draft },
                            set: { store.setAIPrototypeDraft($0) }
                        ),
                        accessibilityID: "aiPrototype.input",
                        onSubmit: sendDraft
                    )
                }
                .frame(minHeight: 58, maxHeight: layout == .expanded ? 100 : 74)
                .background(Color(nsColor: .controlBackgroundColor))
                .clipShape(RoundedRectangle(cornerRadius: 8))
                .overlay(RoundedRectangle(cornerRadius: 8).stroke(Color.secondary.opacity(0.24)))

                if projection.responseState == .streaming {
                    Button("Cancel") { store.cancelAIPrototypeResponse() }
                        .accessibilityIdentifier("aiPrototype.response.cancel")
                } else {
                    Button {
                        store.sendAIPrototypePrompt(projection.draft)
                    } label: {
                        Image(systemName: "arrow.up")
                    }
                    .buttonStyle(.borderedProminent)
                    .tint(aiPrototypePurple)
                    .disabled(
                        projection.draft.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
                            || projection.corpusState != .ready
                    )
                    .help("Send")
                    .accessibilityLabel("Send")
                    .accessibilityIdentifier("aiPrototype.send")
                }
            }

            primaryControls(projection)

            Text("Return to send · Shift-Return for newline")
                .workbenchFont(.caption2)
                .foregroundStyle(.secondary)
        }
        .padding(10)
        .background(.bar)
    }

    private func primaryControls(_ projection: PrototypeAIChatProjection) -> some View {
        HStack(spacing: 10) {
            Menu {
                ForEach(projection.selectedAgent?.models ?? [], id: \.self) { model in
                    Button(model) { store.selectAIPrototypeModel(model) }
                }
            } label: {
                Label(projection.selectedModel, systemImage: "cpu")
                    .lineLimit(1)
            }
            .menuStyle(.borderlessButton)
            .workbenchFont(.caption, weight: .semibold)
            .foregroundStyle(aiPrototypePurple)
            .accessibilityLabel("Model")
            .accessibilityValue(projection.selectedModel)
            .accessibilityIdentifier("aiPrototype.model")

            Menu {
                ForEach(PrototypeAIReasoningEffort.allCases) { effort in
                    Button(effort.label) {
                        store.selectAIPrototypeReasoningEffort(effort)
                    }
                }
            } label: {
                Label(projection.reasoningEffort.label, systemImage: "brain.head.profile")
                    .lineLimit(1)
            }
            .menuStyle(.borderlessButton)
            .workbenchFont(.caption, weight: .semibold)
            .foregroundStyle(aiPrototypePurple)
            .accessibilityLabel("Reasoning effort")
            .accessibilityValue(projection.reasoningEffort.label)
            .accessibilityIdentifier("aiPrototype.effort")

            Spacer(minLength: 0)

            Button {
                usageOpen.toggle()
            } label: {
                Label(projection.usage.compactLabel, systemImage: "gauge.with.dots.needle.67percent")
                    .lineLimit(1)
            }
            .buttonStyle(.plain)
            .workbenchFont(.caption, weight: .semibold)
            .foregroundStyle(aiPrototypePurple)
            .help("Codex subscription usage remaining")
            .accessibilityLabel("Usage remaining")
            .accessibilityValue(projection.usage.compactLabel)
            .accessibilityIdentifier("aiPrototype.usage")
            .popover(isPresented: $usageOpen, arrowEdge: .bottom) {
                usagePanel(projection)
            }

            Button {
                settingsOpen.toggle()
            } label: {
                Image(systemName: "slider.horizontal.3")
            }
            .buttonStyle(.plain)
            .foregroundStyle(aiPrototypePurple)
            .help("Agent, account, access, and Python settings")
            .accessibilityLabel("AI settings")
            .accessibilityIdentifier("aiPrototype.settings")
            .popover(isPresented: $settingsOpen, arrowEdge: .bottom) {
                settingsPanel(projection)
            }
        }
    }

    private func settingsPanel(_ projection: PrototypeAIChatProjection) -> some View {
        VStack(alignment: .leading, spacing: 14) {
            Label("AI settings", systemImage: "sparkles")
                .workbenchFont(.headline)
                .foregroundStyle(aiPrototypePurple)
                .accessibilityIdentifier("aiPrototype.settingsPanel")

            Menu {
                ForEach(projection.agents) { agent in
                    Button(agent.enabled ? agent.label : "\(agent.label) · later") {
                        store.selectAIPrototypeAgent(agent.id)
                    }
                    .disabled(!agent.enabled)
                }
            } label: {
                HStack {
                    Text("Agent")
                    Spacer()
                    Text(projection.selectedAgent?.label ?? "Agent")
                }
            }
            .menuStyle(.borderlessButton)
            .accessibilityLabel("Agent")
            .accessibilityValue(projection.selectedAgent?.label ?? "Agent")
            .accessibilityIdentifier("aiPrototype.agent")

            Divider()

            VStack(alignment: .leading, spacing: 4) {
                HStack {
                    Text("ChatGPT subscription")
                    Spacer()
                    Text(projection.account.label)
                }
                    .accessibilityElement(children: .combine)
                    .accessibilityIdentifier("aiPrototype.account")
                Text(projection.account.status)
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                Text("Codex owns sign-in; CASA-RS stores no credential.")
                    .workbenchFont(.caption2)
                    .foregroundStyle(.secondary)
            }

            Divider()

            Menu {
                Button(PrototypeAITrustPreset.explore.label) {
                    store.selectAIPrototypeTrustPreset(.explore)
                }
                Button(PrototypeAITrustPreset.work.label) {
                    store.selectAIPrototypeTrustPreset(.work)
                }
                Divider()
                Button(PrototypeAITrustPreset.fullAccess.label) {
                    settingsOpen = false
                    confirmingFullAccess = true
                }
            } label: {
                HStack {
                    Text("Access")
                    Spacer()
                    Label(projection.trustPreset.label, systemImage: trustIcon(projection.trustPreset))
                        .lineLimit(1)
                }
            }
            .menuStyle(.borderlessButton)
            .accessibilityLabel("Trust preset")
            .accessibilityValue(projection.trustPreset.label)
            .accessibilityIdentifier("aiPrototype.trust")

            Menu {
                ForEach(projection.pythonEnvironments) { environment in
                    Button(environment.label) {
                        store.selectAIPrototypePythonEnvironment(environment.id)
                    }
                }
            } label: {
                HStack {
                    Text("Scientific Python")
                    Spacer()
                    Text(projection.selectedPythonEnvironment?.label ?? "Python")
                        .lineLimit(1)
                }
            }
            .menuStyle(.borderlessButton)
            .accessibilityLabel("Scientific Python")
            .accessibilityValue(projection.selectedPythonEnvironment?.label ?? "Python")
            .accessibilityIdentifier("aiPrototype.python")
        }
        .padding(16)
        .frame(width: 360)
    }

    private func usagePanel(_ projection: PrototypeAIChatProjection) -> some View {
        VStack(alignment: .leading, spacing: 14) {
            Label("Codex usage remaining", systemImage: "gauge.with.dots.needle.67percent")
                .workbenchFont(.headline)
                .foregroundStyle(aiPrototypePurple)

            usageWindow(
                title: "5-hour window",
                percent: projection.usage.fiveHourRemainingPercent,
                reset: projection.usage.fiveHourReset
            )
            usageWindow(
                title: "Weekly window",
                percent: projection.usage.weeklyRemainingPercent,
                reset: projection.usage.weeklyReset
            )

            Text("Fixture values · production reads account rate limits from Codex.")
                .workbenchFont(.caption2)
                .foregroundStyle(.secondary)
        }
        .padding(16)
        .frame(width: 330)
        .accessibilityIdentifier("aiPrototype.usagePanel")
    }

    private func usageWindow(title: String, percent: Int, reset: String) -> some View {
        VStack(alignment: .leading, spacing: 5) {
            HStack {
                Text(title).workbenchFont(.caption, weight: .semibold)
                Spacer()
                Text("\(percent)%")
                    .workbenchFont(.caption, weight: .semibold)
            }
            ProgressView(value: Double(percent), total: 100)
                .tint(aiPrototypePurple)
            Text(reset)
                .workbenchFont(.caption2)
                .foregroundStyle(.secondary)
        }
    }

    private func contextDisclosure(_ projection: PrototypeAIChatProjection) -> some View {
        Button {
            contextOpen.toggle()
        } label: {
            HStack(spacing: 5) {
                Image(systemName: "chevron.right")
                    .rotationEffect(.degrees(contextOpen ? 90 : 0))
                Text("CASA context available · \(projection.openTabSources.count) open tabs · corpus + source")
                    .lineLimit(1)
                Spacer(minLength: 0)
            }
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .workbenchFont(.caption)
        .accessibilityIdentifier("aiPrototype.contextPreview")
    }

    private func sendDraft() {
        guard let projection = store.state.prototypeAI,
              projection.responseState != .streaming,
              projection.corpusState == .ready,
              !projection.draft.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        else { return }
        store.sendAIPrototypePrompt(projection.draft)
    }

    private func contextPanel(_ projection: PrototypeAIChatProjection) -> some View {
        VStack(spacing: 0) {
            HStack {
                Text("Available through CASA MCP")
                    .workbenchFont(.headline)
                Spacer()
                Button {
                    contextOpen = false
                } label: {
                    Image(systemName: "xmark")
                }
                .buttonStyle(.borderless)
                .accessibilityLabel("Close context")
                .accessibilityIdentifier("aiPrototype.context.close")
            }
            .padding(12)

            Divider()

            ScrollView {
                VStack(alignment: .leading, spacing: 10) {
                    Text("Codex chooses relevant typed resources as it works. CASA records used domain tools and citations, not an exact hidden model prompt or provider-egress manifest.")
                        .workbenchFont(.caption)
                        .foregroundStyle(.secondary)

                    ForEach(projection.workspaceSources) { source in
                        HStack(alignment: .top, spacing: 6) {
                            Image(systemName: source.openTab ? "rectangle.on.rectangle" : "books.vertical")
                                .foregroundStyle(.secondary)
                            VStack(alignment: .leading, spacing: 1) {
                                Text(source.label).workbenchFont(.caption, weight: .semibold)
                                Text(source.detail).workbenchFont(.caption2).foregroundStyle(.secondary)
                            }
                        }
                        .accessibilityIdentifier("aiPrototype.workspaceSource.\(source.id)")
                    }

                    Divider()
                    ForEach(projection.contexts) { context in
                        VStack(alignment: .leading, spacing: 1) {
                            Text(context.label).workbenchFont(.caption, weight: .semibold)
                            Text(context.detail).workbenchFont(.caption2).foregroundStyle(.secondary)
                        }
                        .accessibilityIdentifier("aiPrototype.context.\(context.id)")
                    }

                    corpusControl(projection)
                    Text("Selected preset: \(projection.trustPreset.detail)")
                        .workbenchFont(.caption2)
                        .foregroundStyle(.secondary)
                }
                .padding(14)
            }
        }
        .frame(width: layout == .drawer ? 380 : 420, height: 520)
        .background(Color(nsColor: .windowBackgroundColor))
        .clipShape(RoundedRectangle(cornerRadius: 10))
        .overlay(RoundedRectangle(cornerRadius: 10).stroke(Color.secondary.opacity(0.3)))
        .shadow(radius: 12)
    }

    private func corpusControl(_ projection: PrototypeAIChatProjection) -> some View {
        HStack(spacing: 7) {
            Circle()
                .fill(activityColor(projection.corpusState))
                .frame(width: 7, height: 7)
            Text(corpusLabel(projection.corpusState))
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
                .accessibilityIdentifier("aiPrototype.corpusState")
            Spacer()
            if projection.corpusState == .indexing {
                ProgressView().controlSize(.small)
                Button("Cancel") { store.cancelAIPrototypeIndexing() }
                    .buttonStyle(.link)
                    .accessibilityIdentifier("aiPrototype.index.cancel")
            } else {
                Button(projection.corpusState == .ready ? "Re-index" : "Retry") {
                    store.startAIPrototypeIndexing()
                }
                .buttonStyle(.link)
                .accessibilityIdentifier("aiPrototype.index.start")
            }
        }
    }

    private var fullAccessSheet: some View {
        VStack(alignment: .leading, spacing: 14) {
            Label("Enable Full access?", systemImage: "exclamationmark.shield.fill")
                .workbenchFont(.title2, weight: .semibold)
                .foregroundStyle(.orange)
                .accessibilityIdentifier("aiPrototype.fullAccessSheet")
            Text("This fixture represents unrestricted coding-agent authority. In the real app, Codex could read and change files, execute commands, and use the network without normal Work-mode prompts.")
            Text("CASA-RS will keep Full access visibly indicated until you leave it.")
                .foregroundStyle(.secondary)
            Spacer()
            HStack {
                Button("Cancel") { confirmingFullAccess = false }
                Spacer()
                Button("Enable Full access") {
                    store.selectAIPrototypeTrustPreset(.fullAccess)
                    confirmingFullAccess = false
                }
                .buttonStyle(.borderedProminent)
                .accessibilityIdentifier("aiPrototype.fullAccess.confirm")
            }
        }
        .padding(24)
        .frame(width: 520, height: 290)
    }

    private func sourceSheet(_ citation: PrototypeAICitation) -> some View {
        VStack(alignment: .leading, spacing: 14) {
            Label("Fixture source preview", systemImage: "doc.text.magnifyingglass")
                .workbenchFont(.title2, weight: .semibold)
                .accessibilityIdentifier("aiPrototype.sourceSheet")
            Text(citation.label).workbenchFont(.headline)
            Text(citation.locator).workbenchFont(.body, design: .monospaced)
            Text(citation.excerpt).textSelection(.enabled)
            Spacer()
            HStack {
                Spacer()
                Button("Done") { sourceCitation = nil }
                    .keyboardShortcut(.defaultAction)
            }
        }
        .padding(24)
        .frame(width: 620, height: 360)
    }

    private func selectedCitation(in message: PrototypeAIMessage) -> PrototypeAICitation? {
        guard let selectedCitationID else { return nil }
        return message.citations.first { $0.id == selectedCitationID }
    }

    private func contextLabels(_ ids: [String], projection: PrototypeAIChatProjection) -> String {
        ids.compactMap { id in projection.contexts.first { $0.id == id }?.label }
            .joined(separator: " · ")
    }

    private func corpusLabel(_ state: PrototypeAIActivityState) -> String {
        switch state {
        case .ready: "Local corpus ready · FTS fixture"
        case .indexing: "Indexing local sources"
        case .offline: "Local corpus offline"
        case .cancelled: "Indexing cancelled"
        case .failed: "Indexing failed"
        default: state.rawValue.capitalized
        }
    }

    private func responseFailureLabel(_ state: PrototypeAIActivityState) -> String {
        switch state {
        case .rateLimited: "Codex rate limited; nothing was retried."
        case .offline: "Codex unavailable; project context remains local."
        default: "Agent failed before returning an answer."
        }
    }

    private func activityColor(_ state: PrototypeAIActivityState) -> Color {
        switch state {
        case .ready, .completed: .green
        case .indexing, .streaming: .blue
        case .offline, .failed, .rateLimited, .restartRequired: .orange
        case .cancelled: .secondary
        default: .secondary
        }
    }

    private func trustIcon(_ preset: PrototypeAITrustPreset) -> String {
        switch preset {
        case .explore: "eye"
        case .work: "hammer"
        case .fullAccess: "exclamationmark.shield"
        }
    }

    private func suggestionID(_ prompt: String) -> String {
        if prompt.contains("current plot") { return "plot" }
        if prompt.contains("Imager") { return "task" }
        return "data-types"
    }
}

struct AIComposerEditor: NSViewRepresentable {
    @Binding var text: String
    let accessibilityID: String
    let onSubmit: () -> Void

    func makeCoordinator() -> Coordinator {
        Coordinator(text: $text)
    }

    func makeNSView(context: Context) -> NSScrollView {
        let textView = UserActivatedTextView()
        textView.delegate = context.coordinator
        textView.string = text
        textView.isRichText = false
        textView.isEditable = true
        textView.isSelectable = true
        textView.drawsBackground = false
        textView.font = .preferredFont(forTextStyle: .body)
        textView.textContainerInset = NSSize(width: 5, height: 6)
        textView.isVerticallyResizable = true
        textView.isHorizontallyResizable = false
        textView.autoresizingMask = [.width]
        textView.textContainer?.widthTracksTextView = true
        textView.isAutomaticTextCompletionEnabled = false
        textView.isAutomaticSpellingCorrectionEnabled = false
        textView.isAutomaticTextReplacementEnabled = false
        textView.isAutomaticQuoteSubstitutionEnabled = false
        textView.isAutomaticDashSubstitutionEnabled = false
        textView.setAccessibilityLabel("Ask anything about this project")
        textView.setAccessibilityHelp("Return sends. Shift-Return inserts a new line.")
        textView.setAccessibilityIdentifier(accessibilityID)
        textView.onSubmit = onSubmit

        let scrollView = NSScrollView()
        scrollView.drawsBackground = false
        scrollView.hasVerticalScroller = true
        scrollView.autohidesScrollers = true
        scrollView.borderType = .noBorder
        scrollView.documentView = textView
        return scrollView
    }

    func updateNSView(_ scrollView: NSScrollView, context: Context) {
        guard let textView = scrollView.documentView as? UserActivatedTextView else { return }
        textView.onSubmit = onSubmit
        if textView.string != text {
            textView.string = text
        }
    }

    final class Coordinator: NSObject, NSTextViewDelegate {
        private var text: Binding<String>

        init(text: Binding<String>) {
            self.text = text
        }

        func textDidChange(_ notification: Notification) {
            guard let textView = notification.object as? NSTextView else { return }
            text.wrappedValue = textView.string
        }
    }
}

/// Prevents AppKit from opening its completion UI merely because the composer
/// appeared. A real click enables ordinary keyboard focus and editing.
private final class UserActivatedTextView: NSTextView {
    private var userRequestedFocus = false
    var onSubmit: (() -> Void)?

    override var acceptsFirstResponder: Bool { userRequestedFocus }

    override func mouseDown(with event: NSEvent) {
        userRequestedFocus = true
        window?.makeFirstResponder(self)
        super.mouseDown(with: event)
    }

    override func keyDown(with event: NSEvent) {
        let isReturn = event.keyCode == 36 || event.keyCode == 76
        if isReturn, !event.modifierFlags.contains(.shift) {
            onSubmit?()
            return
        }
        super.keyDown(with: event)
    }

    override func insertNewline(_ sender: Any?) {
        if NSApp.currentEvent?.modifierFlags.contains(.shift) == true {
            super.insertNewline(sender)
        } else {
            onSubmit?()
        }
    }

    override func insertText(_ insertString: Any, replacementRange: NSRange) {
        let string = insertString as? String
        let isNewline = string == "\n" || string == "\r"
        if isNewline, NSApp.currentEvent?.modifierFlags.contains(.shift) != true {
            onSubmit?()
            return
        }
        super.insertText(insertString, replacementRange: replacementRange)
    }
}
