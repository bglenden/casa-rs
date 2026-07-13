import AppKit
import CasarsMacCore
import SwiftUI

enum AssistantDiscussionLayout {
    case drawer
    case expanded
}

struct AssistantDiscussionView: View {
    @ObservedObject var store: WorkbenchStore
    let layout: AssistantDiscussionLayout

    @State private var contextsExpanded = false
    @State private var expandedCitationIDs: Set<String> = []
    @State private var authenticationValue = ""
    @State private var pendingPinMessage: AssistantMessageState?
    @State private var pinRepresentation = "answer_with_citations"
    @State private var pinNotebookID = ""

    private var discussion: AssistantDiscussionState? {
        store.state.assistantDiscussion
    }

    var body: some View {
        VStack(spacing: 0) {
            if let discussion {
                header(discussion)
                Divider()
                conversation(discussion)
                Divider()
                composer(discussion)
            } else {
                VStack(spacing: 10) {
                    Image(systemName: "sparkles").workbenchFont(.title3)
                    Text("AI discussion unavailable").workbenchFont(.headline)
                    Text("Open a project to start a persistent discussion.")
                        .foregroundStyle(.secondary)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            }
        }
        .background(Color(nsColor: .textBackgroundColor))
        .accessibilityIdentifier("assistant.discussion")
        .sheet(item: Binding(
            get: { discussion?.pendingAuthenticationPrompt },
            set: { value in
                if value == nil { store.dismissAssistantAuthenticationPrompt() }
            }
        )) { prompt in
            authenticationPrompt(prompt)
        }
        .sheet(item: $pendingPinMessage) { message in
            pinConfirmation(message)
        }
    }

    private func header(_ discussion: AssistantDiscussionState) -> some View {
        HStack(spacing: 9) {
            Image(systemName: "sparkles")
                .foregroundStyle(.tint)
            VStack(alignment: .leading, spacing: 1) {
                Text(layout == .drawer ? "Notebook chat" : discussion.activeConversation?.title ?? "AI discussion")
                    .workbenchFont(.headline)
                Text(primaryAttachmentLabel(discussion))
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }
            Spacer()
            Menu {
                ForEach(discussion.conversations) { conversation in
                    Button(conversation.title) {
                        store.selectAssistantConversation(conversation.id)
                    }
                }
                Divider()
                Button("New discussion", systemImage: "plus") {
                    store.newAssistantConversation()
                }
            } label: {
                Image(systemName: "clock")
            }
            .menuStyle(.borderlessButton)
            .help("Conversation history")
            .accessibilityIdentifier("assistant.history")

            if layout == .drawer {
                Button {
                    store.expandAssistantDiscussion()
                } label: {
                    Image(systemName: "arrow.up.left.and.arrow.down.right")
                }
                .buttonStyle(.borderless)
                .help("Open in AI tab")
                .accessibilityIdentifier("assistant.expand")
                Button {
                    store.closeAssistantDiscussion()
                } label: {
                    Image(systemName: "xmark")
                }
                .buttonStyle(.borderless)
                .help("Close chat")
                .accessibilityIdentifier("assistant.close")
            } else {
                Button("Dock beside notebook") { store.dockAssistantDiscussion() }
                    .controlSize(.small)
                    .accessibilityIdentifier("assistant.dock")
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 10)
        .background(.bar)
    }

    private func conversation(_ discussion: AssistantDiscussionState) -> some View {
        ScrollViewReader { proxy in
            ScrollView {
                LazyVStack(alignment: .leading, spacing: 12) {
                if discussion.activeConversation?.messages.isEmpty != false {
                    VStack(alignment: .leading, spacing: 10) {
                        Label("Ask about this project", systemImage: "bubble.left.and.text.bubble.right")
                            .workbenchFont(.title3, weight: .semibold)
                        Text("I can use bounded projections of every open tab and retrieve cited evidence from the radio astronomy, project-document, and CASA-RS source corpus.")
                            .foregroundStyle(.secondary)
                    }
                    .padding(14)
                    .background(Color.accentColor.opacity(0.06))
                    .clipShape(RoundedRectangle(cornerRadius: 10))
                }
                ForEach(discussion.activeConversation?.messages ?? []) { message in
                    messageRow(message)
                        .id(message.id)
                        .onAppear { store.setAssistantScrollAnchor(message.id) }
                }
                if !assistantProposals(discussion).isEmpty {
                    Button {
                        store.showAssistantNotebookSuggestions()
                    } label: {
                        HStack(spacing: 9) {
                            Image(systemName: "doc.badge.ellipsis")
                                .foregroundStyle(.tint)
                            VStack(alignment: .leading, spacing: 2) {
                                Text("Suggestions are in the notebook")
                                    .workbenchFont(.subheadline, weight: .semibold)
                                Text("Review insertion and execution separately at their destination")
                                    .workbenchFont(.caption)
                                    .foregroundStyle(.secondary)
                            }
                            Spacer()
                            Image(systemName: "arrow.up.forward.app")
                        }
                        .padding(10)
                        .contentShape(Rectangle())
                    }
                    .buttonStyle(.plain)
                    .background(Color.accentColor.opacity(0.06))
                    .clipShape(RoundedRectangle(cornerRadius: 8))
                    .accessibilityIdentifier("assistant.openNotebookSuggestions")
                }
                if discussion.activity == .streaming {
                    HStack(spacing: 8) {
                        ProgressView().controlSize(.small)
                        Text(discussion.streamingText.isEmpty ? "Reading selected context…" : discussion.streamingText)
                            .textSelection(.enabled)
                    }
                    .padding(10)
                    .accessibilityIdentifier("assistant.streaming")
                }
                if let error = discussion.lastError {
                    HStack(alignment: .top, spacing: 8) {
                        Image(systemName: "exclamationmark.triangle.fill")
                            .foregroundStyle(.orange)
                        Text(error).textSelection(.enabled)
                        Spacer()
                        if discussion.activity == .restartRequired {
                            Button("Restart") { store.restartAssistantSidecar() }
                        }
                    }
                    .padding(10)
                    .background(Color.orange.opacity(0.08))
                    .clipShape(RoundedRectangle(cornerRadius: 8))
                    .accessibilityIdentifier("assistant.error")
                }
                }
                .frame(maxWidth: layout == .expanded ? 760 : .infinity, alignment: .leading)
                .padding(layout == .expanded ? 24 : 12)
                .frame(maxWidth: .infinity)
            }
            .onAppear {
                if let anchor = discussion.activeConversation?.scrollAnchorMessageId {
                    DispatchQueue.main.async { proxy.scrollTo(anchor, anchor: .center) }
                }
            }
            .accessibilityIdentifier("assistant.conversationScroll")
        }
    }

    private func messageRow(_ message: AssistantMessageState) -> some View {
        HStack {
            if message.role == "user" { Spacer(minLength: layout == .expanded ? 120 : 28) }
            VStack(alignment: .leading, spacing: 7) {
                HStack {
                    Text(message.role == "user" ? "You" : "Assistant")
                        .workbenchFont(.caption, weight: .semibold)
                        .foregroundStyle(.secondary)
                    if let provider = message.provider, let model = message.model {
                        Text("\(provider) · \(model)")
                            .workbenchFont(.caption2)
                            .foregroundStyle(.tertiary)
                    }
                    Spacer()
                }
                Text(message.content)
                    .textSelection(.enabled)
                if !message.citations.isEmpty {
                    ForEach(Array(message.citations.enumerated()), id: \.element.id) { index, citation in
                        DisclosureGroup(
                            isExpanded: Binding(
                                get: { expandedCitationIDs.contains(citation.id) },
                                set: { expanded in
                                    if expanded { expandedCitationIDs.insert(citation.id) }
                                    else { expandedCitationIDs.remove(citation.id) }
                                }
                            )
                        ) {
                            VStack(alignment: .leading, spacing: 4) {
                                Text(citation.locator).workbenchFont(.caption, weight: .semibold)
                                Text(citation.excerpt).workbenchFont(.caption).textSelection(.enabled)
                            }
                            .padding(.leading, 6)
                        } label: {
                            Text("[\(index + 1)] \(citation.label)")
                                .workbenchFont(.caption)
                        }
                        .accessibilityIdentifier("assistant.citation.\(citation.id)")
                    }
                }
                if let egress = message.egress {
                    DisclosureGroup("Sent context · \(ByteCountFormatter.string(fromByteCount: Int64(egress.estimatedBytes), countStyle: .file)) → \(egress.destination)") {
                        Text(egress.items.filter(\.providerVisible).map(\.label).joined(separator: ", "))
                            .workbenchFont(.caption2)
                            .foregroundStyle(.secondary)
                    }
                    .workbenchFont(.caption)
                }
                if message.role == "assistant" {
                    HStack {
                        if let pin = message.pins.first {
                            Label("Pinned snapshot", systemImage: "pin.fill")
                                .workbenchFont(.caption)
                                .foregroundStyle(.secondary)
                            Text(String(pin.contentSha256.prefix(10)))
                                .workbenchFont(.caption2, design: .monospaced)
                                .foregroundStyle(.tertiary)
                        } else {
                            Button {
                                pinRepresentation = message.citations.isEmpty
                                    ? "answer_only" : "answer_with_citations"
                                pinNotebookID = store.state.scientificNotebooks?.activeNotebookID ?? ""
                                pendingPinMessage = message
                            } label: {
                                Label("Pin to notebook", systemImage: "pin")
                            }
                            .buttonStyle(.link)
                            .accessibilityIdentifier("assistant.message.\(message.id).pin")
                        }
                    }
                }
            }
            .padding(11)
            .background(message.role == "user" ? Color.accentColor.opacity(0.12) : Color.secondary.opacity(0.08))
            .clipShape(RoundedRectangle(cornerRadius: 10))
            if message.role != "user" { Spacer(minLength: layout == .expanded ? 70 : 0) }
        }
        .accessibilityIdentifier("assistant.message.\(message.id)")
    }

    private func composer(_ discussion: AssistantDiscussionState) -> some View {
        VStack(alignment: .leading, spacing: 8) {
            DisclosureGroup(isExpanded: $contextsExpanded) {
                VStack(alignment: .leading, spacing: 7) {
                    ForEach(discussion.contexts) { context in
                        Toggle(isOn: Binding(
                            get: { context.providerVisible },
                            set: { _ in store.toggleAssistantContext(context.id) }
                        )) {
                            VStack(alignment: .leading, spacing: 1) {
                                Text(context.label).workbenchFont(.caption, weight: .semibold)
                                Text(context.summary).workbenchFont(.caption2).foregroundStyle(.secondary)
                            }
                        }
                        .toggleStyle(.checkbox)
                        .accessibilityIdentifier("assistant.context.\(context.id)")
                    }
                    Text("Bulk arrays, raw visibilities, credentials, and unrestricted files are excluded.")
                        .workbenchFont(.caption2)
                        .foregroundStyle(.secondary)
                    HStack {
                        Text(discussion.corpusStatus)
                            .workbenchFont(.caption2)
                            .foregroundStyle(.secondary)
                        Spacer()
                        Button("Refresh local corpus") { store.refreshAssistantCorpus() }
                            .controlSize(.small)
                            .disabled(discussion.corpusStatus == "Indexing local corpus…")
                            .accessibilityIdentifier("assistant.corpus.refresh")
                    }
                }
                .padding(.top, 6)
            } label: {
                Text(egressLabel(discussion))
                    .workbenchFont(.caption)
            }
            .accessibilityIdentifier("assistant.egress")

            HStack(alignment: .bottom, spacing: 8) {
                ZStack(alignment: .topLeading) {
                    if discussion.activeConversation?.draft.isEmpty != false {
                        Text("Ask anything about this project…")
                            .foregroundStyle(.tertiary)
                            .padding(.horizontal, 6)
                            .padding(.vertical, 8)
                    }
                    AIComposerEditor(
                        text: Binding(
                            get: { discussion.activeConversation?.draft ?? "" },
                            set: { store.setAssistantDraft($0) }
                        ),
                        accessibilityID: "assistant.input",
                        onSubmit: store.sendAssistantPrompt
                    )
                }
                .frame(minHeight: 58, maxHeight: layout == .expanded ? 100 : 74)
                .background(Color(nsColor: .controlBackgroundColor))
                .clipShape(RoundedRectangle(cornerRadius: 8))
                .overlay(RoundedRectangle(cornerRadius: 8).stroke(Color.secondary.opacity(0.24)))
                if discussion.activity == .streaming {
                    Button("Cancel") { store.cancelAssistantResponse() }
                        .accessibilityIdentifier("assistant.cancel")
                } else {
                    Button {
                        store.sendAssistantPrompt()
                    } label: {
                        Image(systemName: "arrow.up")
                    }
                    .buttonStyle(.borderedProminent)
                    .disabled(
                        discussion.activeConversation?.draft.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty != false
                            || ![.ready, .completed].contains(discussion.activity)
                    )
                    .accessibilityLabel("Send")
                    .accessibilityIdentifier("assistant.send")
                }
            }

            HStack(spacing: 10) {
                Menu {
                    ForEach(discussion.providers) { provider in
                        Button(provider.label) { store.selectAssistantProvider(provider.id) }
                    }
                } label: {
                    Label(discussion.selectedProvider?.label ?? "Provider", systemImage: "person.crop.circle")
                        .lineLimit(1)
                }
                .menuStyle(.borderlessButton)
                .workbenchFont(.caption)
                .accessibilityIdentifier("assistant.provider")

                Menu {
                    ForEach(discussion.selectedProvider?.models ?? []) { model in
                        Button(model.label) { store.selectAssistantModel(model.id) }
                    }
                } label: {
                    Text(selectedModelLabel(discussion)).lineLimit(1)
                }
                .menuStyle(.borderlessButton)
                .workbenchFont(.caption)
                .accessibilityIdentifier("assistant.model")

                if let provider = discussion.selectedProvider, !provider.configured {
                    Button("Sign in") { store.authenticateAssistantProvider(provider.id) }
                        .controlSize(.small)
                        .accessibilityIdentifier("assistant.authenticate")
                }
                Spacer(minLength: 0)
                Text("Return to send · Shift-Return for newline")
                    .workbenchFont(.caption2)
                    .foregroundStyle(.secondary)
            }

            if let url = discussion.pendingAuthenticationURL,
               let destination = URL(string: url)
            {
                HStack {
                    Text(discussion.pendingAuthenticationInstructions ?? "Complete sign-in in your browser.")
                        .workbenchFont(.caption)
                    Spacer()
                    Button("Open sign-in") { NSWorkspace.shared.open(destination) }
                        .controlSize(.small)
                }
                .padding(8)
                .background(Color.accentColor.opacity(0.07))
                .clipShape(RoundedRectangle(cornerRadius: 7))
            }
        }
        .padding(10)
        .background(.bar)
    }

    private func authenticationPrompt(_ prompt: AssistantAuthenticationPromptState) -> some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("Provider sign-in").workbenchFont(.title3, weight: .semibold)
            Text(prompt.message)
            if prompt.secret {
                SecureField("Value", text: $authenticationValue)
            } else {
                TextField("Value", text: $authenticationValue)
            }
            HStack {
                Spacer()
                Button("Continue") {
                    store.submitAssistantAuthenticationPrompt(
                        requestID: prompt.requestID,
                        promptID: prompt.promptID,
                        value: authenticationValue
                    )
                    authenticationValue = ""
                }
                .buttonStyle(.borderedProminent)
                .disabled(authenticationValue.isEmpty)
            }
        }
        .padding(22)
        .frame(width: 460)
    }

    private func pinConfirmation(_ message: AssistantMessageState) -> some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("Pin immutable snapshot").workbenchFont(.title3, weight: .semibold)
            Text("Choose the representation, then confirm the exact Markdown appended to the active notebook. Later chat changes will not update it.")
                .foregroundStyle(.secondary)
            Picker("Representation", selection: $pinRepresentation) {
                Text("Answer only").tag("answer_only")
                Text("Answer with citations").tag("answer_with_citations")
            }
            .pickerStyle(.segmented)
            .disabled(message.citations.isEmpty)
            Picker("Notebook", selection: $pinNotebookID) {
                ForEach(store.state.scientificNotebooks?.notebooks ?? []) { notebook in
                    Text(notebook.title).tag(notebook.id)
                }
            }
            .accessibilityIdentifier("assistant.pin.notebook")
            ScrollView {
                Text(store.assistantPinPreview(
                    message,
                    representation: pinRepresentation,
                    notebookID: pinNotebookID
                ))
                    .workbenchFont(.caption, design: .monospaced)
                    .textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .padding(10)
            }
            .frame(height: 240)
            .background(Color(nsColor: .controlBackgroundColor))
            .clipShape(RoundedRectangle(cornerRadius: 7))
            Text("Destination: selected notebook · chronological tail")
                .workbenchFont(.caption)
                .foregroundStyle(.secondary)
            HStack {
                Button("Cancel") { pendingPinMessage = nil }
                Spacer()
                Button("Pin snapshot") {
                    store.pinAssistantMessage(
                        message.id,
                        representation: pinRepresentation,
                        notebookID: pinNotebookID
                    )
                    pendingPinMessage = nil
                }
                .buttonStyle(.borderedProminent)
                .accessibilityIdentifier("assistant.pin.confirm")
            }
        }
        .padding(22)
        .frame(width: 620, height: 470)
    }

    private func primaryAttachmentLabel(_ discussion: AssistantDiscussionState) -> String {
        guard let attachment = discussion.activeConversation?.attachments.first(where: \.primary) else {
            return "Attached to this project"
        }
        return "Attached to \(attachment.label)"
    }

    private func egressLabel(_ discussion: AssistantDiscussionState) -> String {
        let visible = discussion.contexts.filter(\.providerVisible)
        let bytes = visible.reduce(UInt64(0)) { $0 + $1.byteCount }
        let destination = discussion.activeConversation.map { providerDestination($0.provider) } ?? "provider"
        return "Context: \(visible.count) items · \(ByteCountFormatter.string(fromByteCount: Int64(bytes), countStyle: .file)) → \(destination)"
    }

    private func providerDestination(_ provider: String) -> String {
        switch provider {
        case "openai-codex": "chatgpt.com"
        case "openai": "api.openai.com"
        case "opencode": "opencode.ai"
        default: provider
        }
    }

    private func selectedModelLabel(_ discussion: AssistantDiscussionState) -> String {
        guard let conversation = discussion.activeConversation else { return "Model" }
        return discussion.selectedProvider?.models.first(where: { $0.id == conversation.model })?.label
            ?? conversation.model
    }

    private func assistantProposals(_ discussion: AssistantDiscussionState) -> [AssistantProposalState] {
        discussion.activeConversation?.messages.flatMap(\.proposals) ?? []
    }
}
