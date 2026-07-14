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
    @State private var settingsPresented = false
    @State private var agentCommandDraft = ""
    @State private var pythonCommandDraft = ""
    @State private var confirmFullAccess = false

    private var discussion: AssistantDiscussionState? { store.state.assistantDiscussion }

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
        .tint(.purple)
        .background(Color(nsColor: .textBackgroundColor))
        .confirmationDialog(
            "Give this agent full system access?",
            isPresented: $confirmFullAccess,
            titleVisibility: .visible
        ) {
            Button("Use Full access", role: .destructive) {
                store.selectAssistantAuthority(.fullAccess)
            }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text("The coding agent can run commands and change files without approval prompts. CASA task and notebook actions still remain explicit.")
        }
    }

    private func header(_ discussion: AssistantDiscussionState) -> some View {
        HStack(spacing: 9) {
            Image(systemName: "sparkles").foregroundStyle(.purple)
            VStack(alignment: .leading, spacing: 1) {
                Text(layout == .drawer ? "Notebook chat" : discussion.activeConversation?.title ?? "AI discussion")
                    .workbenchFont(.headline)
                    .accessibilityIdentifier("assistant.discussion")
                Text(primaryAttachmentLabel(discussion))
                    .workbenchFont(.caption)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
            }
            Spacer()
            Menu {
                ForEach(discussion.conversations) { conversation in
                    Button(conversation.title) { store.selectAssistantConversation(conversation.id) }
                }
                Divider()
                Button("New discussion", systemImage: "plus") { store.newAssistantConversation() }
            } label: { Image(systemName: "clock") }
                .menuStyle(.borderlessButton)
                .help("Conversation history")
                .accessibilityIdentifier("assistant.history")
            if layout == .drawer {
                Button { store.expandAssistantDiscussion() } label: {
                    Image(systemName: "arrow.up.left.and.arrow.down.right")
                }
                .buttonStyle(.borderless)
                .help("Open in AI tab")
                Button { store.closeAssistantDiscussion() } label: { Image(systemName: "xmark") }
                    .buttonStyle(.borderless)
                    .help("Close chat")
                    .accessibilityIdentifier("assistant.close")
            } else {
                Button("Dock beside notebook") { store.dockAssistantDiscussion() }
                    .controlSize(.small)
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 10)
        .background(.bar)
    }

    private func conversation(_ discussion: AssistantDiscussionState) -> some View {
        ScrollViewReader { proxy in
            ScrollView {
                // Chat transcripts are modest in size and update rapidly while a turn streams.
                // A LazyVStack can repeatedly invalidate its view-list layout when the streaming
                // row follows citation disclosure groups, driving AttributeGraph into a hot loop.
                VStack(alignment: .leading, spacing: 12) {
                    if discussion.activeConversation?.messages.isEmpty != false {
                        VStack(alignment: .leading, spacing: 8) {
                            Label("Ask about this project", systemImage: "bubble.left.and.text.bubble.right")
                                .workbenchFont(.title3, weight: .semibold)
                            Text("The agent can use open tabs, CASA task schemas, cited radio-astronomy documents, project papers, and the current casa-rs source corpus.")
                                .foregroundStyle(.secondary)
                        }
                        .padding(14)
                        .background(Color.purple.opacity(0.06))
                        .clipShape(RoundedRectangle(cornerRadius: 10))
                    }
                    ForEach(discussion.activeConversation?.messages ?? []) { message in
                        messageRow(message).id(message.id)
                    }
                    if discussion.activity == .streaming {
                        HStack(alignment: .top, spacing: 8) {
                            Image(systemName: "sparkles")
                                .foregroundStyle(.purple)
                            Text(discussion.streamingText.isEmpty ? "Thinking…" : discussion.streamingText)
                                .textSelection(.enabled)
                        }
                        .padding(10)
                        .accessibilityIdentifier("assistant.streaming")
                    }
                    if let approval = discussion.pendingApproval {
                        VStack(alignment: .leading, spacing: 8) {
                            Label("Agent requests approval", systemImage: "checkmark.shield")
                                .workbenchFont(.subheadline, weight: .semibold)
                            Text(approval.summary).workbenchFont(.caption).textSelection(.enabled)
                            HStack {
                                Button("Deny") { store.resolveAssistantApproval("decline") }
                                Button("Approve") { store.resolveAssistantApproval("accept") }
                                    .buttonStyle(.borderedProminent)
                            }
                        }
                        .padding(10)
                        .background(Color.purple.opacity(0.08))
                        .clipShape(RoundedRectangle(cornerRadius: 8))
                        .accessibilityIdentifier("assistant.approval")
                    }
                    if let error = discussion.lastError {
                        HStack(alignment: .top, spacing: 8) {
                            Image(systemName: "exclamationmark.triangle.fill").foregroundStyle(.orange)
                            Text(error).textSelection(.enabled)
                            Spacer()
                            Button("Restart") { store.restartAssistantAgent() }
                        }
                        .padding(10)
                        .background(Color.orange.opacity(0.08))
                        .clipShape(RoundedRectangle(cornerRadius: 8))
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
                    if let model = message.model {
                        Text(model).workbenchFont(.caption2).foregroundStyle(.tertiary)
                    }
                    Spacer()
                }
                if let rendered = NotebookMarkdownPresentation.attributedString(message.content) {
                    Text(rendered).textSelection(.enabled)
                }
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
                        Text(citation.excerpt).workbenchFont(.caption).textSelection(.enabled)
                    } label: {
                        Text("[\(index + 1)] \(citation.label) · \(citation.locator)")
                            .workbenchFont(.caption)
                    }
                }
                ForEach(message.taskSuggestions) { suggestion in
                    Button("Open \(suggestion.taskId) task") {
                        store.openAssistantTaskSuggestion(
                            messageID: message.id,
                            suggestionID: suggestion.id
                        )
                    }
                    .buttonStyle(.link)
                    .accessibilityIdentifier("assistant.message.\(message.id).task.\(suggestion.id)")
                }
                if message.role == "assistant" {
                    if message.pins.isEmpty {
                        Button {
                            store.pinAssistantMessage(message.id)
                        } label: {
                            Label("Add to notebook", systemImage: "text.badge.plus")
                        }
                        .buttonStyle(.link)
                        .accessibilityIdentifier("assistant.message.\(message.id).pin")
                    } else {
                        Label("Added to notebook", systemImage: "checkmark")
                            .workbenchFont(.caption)
                            .foregroundStyle(.secondary)
                    }
                }
            }
            .padding(11)
            .background(message.role == "user" ? Color.purple.opacity(0.12) : Color.secondary.opacity(0.08))
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
                            get: { context.selected },
                            set: { _ in store.toggleAssistantContext(context.id) }
                        )) {
                            VStack(alignment: .leading, spacing: 1) {
                                Text(context.label).workbenchFont(.caption, weight: .semibold)
                                Text(context.summary).workbenchFont(.caption2).foregroundStyle(.secondary)
                            }
                        }
                        .toggleStyle(.checkbox)
                    }
                    HStack {
                        Text(discussion.corpusStatus).workbenchFont(.caption2).foregroundStyle(.secondary)
                        Spacer()
                        Button("Refresh local corpus") { store.refreshAssistantCorpus() }
                            .controlSize(.small)
                    }
                }
                .padding(.top, 6)
            } label: {
                Text("Context: \(discussion.contexts.filter(\.selected).count) project items")
                    .workbenchFont(.caption)
            }

            HStack(alignment: .bottom, spacing: 8) {
                AIComposerEditor(
                    text: Binding(
                        get: { discussion.activeConversation?.draft ?? "" },
                        set: { store.setAssistantDraft($0) }
                    ),
                    accessibilityID: "assistant.input",
                    onSubmit: store.sendAssistantPrompt
                )
                .disabled(discussion.account.requiresLogin)
                .frame(minHeight: 58, maxHeight: layout == .expanded ? 100 : 74)
                .background(Color(nsColor: .controlBackgroundColor))
                .clipShape(RoundedRectangle(cornerRadius: 8))
                .overlay(RoundedRectangle(cornerRadius: 8).stroke(Color.secondary.opacity(0.24)))
                if discussion.activity == .streaming {
                    Button("Cancel") { store.cancelAssistantResponse() }
                } else {
                    Button { store.sendAssistantPrompt() } label: { Image(systemName: "arrow.up") }
                        .buttonStyle(.borderedProminent)
                        .disabled(
                            discussion.account.requiresLogin
                                || discussion.activeConversation?.draft
                                .trimmingCharacters(in: .whitespacesAndNewlines).isEmpty != false
                        )
                        .accessibilityLabel("Send")
                        .accessibilityIdentifier("assistant.send")
                }
            }

            controls(discussion)
        }
        .padding(10)
        .background(.bar)
    }

    private func controls(_ discussion: AssistantDiscussionState) -> some View {
        HStack(spacing: 10) {
            Menu {
                ForEach(discussion.models) { model in
                    Button(model.label) { store.selectAssistantModel(model.id) }
                        .accessibilityIdentifier("assistant.model.option.\(model.id)")
                }
            } label: {
                Text(selectedModelLabel(discussion)).lineLimit(1)
            }
            .menuStyle(.borderlessButton)
            .workbenchFont(.caption)
            .accessibilityLabel(selectedModelLabel(discussion))
            .accessibilityIdentifier("assistant.model")

            Menu {
                ForEach(selectedEfforts(discussion), id: \.self) { effort in
                    Button(effort.capitalized) { store.selectAssistantEffort(effort) }
                }
            } label: {
                Text(discussion.activeConversation?.profile.effort.capitalized ?? "Effort")
            }
            .menuStyle(.borderlessButton)
            .workbenchFont(.caption)
            .accessibilityIdentifier("assistant.effort")

            Spacer()
            if discussion.account.requiresLogin {
                Button("Sign in to ChatGPT") { store.authenticateAssistantAccount() }
                    .controlSize(.small)
                    .accessibilityIdentifier("assistant.account.login")
            } else {
                Text(accountAndUsage(discussion))
                    .workbenchFont(.caption2)
                    .foregroundStyle(.secondary)
                    .accessibilityIdentifier("assistant.usage")
            }
            if discussion.activeConversation?.profile.authority == .fullAccess {
                Label("Full access", systemImage: "exclamationmark.shield.fill")
                    .workbenchFont(.caption2)
                    .foregroundStyle(.orange)
                    .accessibilityIdentifier("assistant.full-access")
            }
            Button {
                agentCommandDraft = discussion.activeConversation?.profile.agentCommand ?? "codex"
                pythonCommandDraft = discussion.activeConversation?.profile.pythonCommand ?? "python3"
                settingsPresented.toggle()
            } label: {
                Image(systemName: "gearshape")
            }
            .buttonStyle(.borderless)
            .help("Agent, account, authority, and Python settings")
            .accessibilityIdentifier("assistant.settings")
            .popover(isPresented: $settingsPresented, arrowEdge: .bottom) {
                assistantSettings(discussion)
            }
        }
    }

    private func assistantSettings(_ discussion: AssistantDiscussionState) -> some View {
        VStack(alignment: .leading, spacing: 14) {
            Text("AI settings").workbenchFont(.headline)
            VStack(alignment: .leading, spacing: 5) {
                Text("Codex agent").workbenchFont(.caption, weight: .semibold)
                HStack {
                    TextField("codex or executable path", text: $agentCommandDraft)
                        .textFieldStyle(.roundedBorder)
                    Button("Apply") { store.setAssistantAgentCommand(agentCommandDraft) }
                        .disabled(agentCommandDraft.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                }
            }
            VStack(alignment: .leading, spacing: 5) {
                Text("Scientific Python").workbenchFont(.caption, weight: .semibold)
                HStack {
                    TextField("python3 or executable path", text: $pythonCommandDraft)
                        .textFieldStyle(.roundedBorder)
                    Button("Apply") { store.setAssistantPythonCommand(pythonCommandDraft) }
                        .disabled(pythonCommandDraft.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                }
                if let python = discussion.activeConversation?.profile.pythonProvenance {
                    Text("\(python.environmentLabel) · \(python.implementation) \(python.version)\n\(python.resolvedPath)")
                        .workbenchFont(.caption2)
                        .foregroundStyle(.secondary)
                        .textSelection(.enabled)
                } else {
                    Text("Environment identity will be recorded after the interpreter is inspected.")
                        .workbenchFont(.caption2)
                        .foregroundStyle(.secondary)
                }
            }
            VStack(alignment: .leading, spacing: 6) {
                Text("Authority").workbenchFont(.caption, weight: .semibold)
                HStack {
                    ForEach(AssistantAuthorityState.allCases) { authority in
                        Button(authority.label) {
                            if authority == .fullAccess {
                                confirmFullAccess = true
                            } else {
                                store.selectAssistantAuthority(authority)
                            }
                        }
                        .buttonStyle(.bordered)
                        .background(
                            discussion.activeConversation?.profile.authority == authority
                                ? Color.purple.opacity(0.14) : Color.clear
                        )
                        .clipShape(RoundedRectangle(cornerRadius: 6))
                    }
                }
            }
            Divider()
            HStack {
                LabeledContent("Account", value: discussion.account.email ?? "ChatGPT subscription")
                if !discussion.account.requiresLogin {
                    Button("Log out") {
                        store.logoutAssistantAccount()
                        settingsPresented = false
                    }
                    .disabled(discussion.activity == .streaming)
                    .accessibilityIdentifier("assistant.account.logout")
                }
            }
            LabeledContent("Plan", value: discussion.account.plan?.capitalized ?? "Unknown")
            Divider()
            VStack(alignment: .leading, spacing: 6) {
                Text("Local corpus").workbenchFont(.caption, weight: .semibold)
                Text(discussion.corpusStatus)
                    .workbenchFont(.caption2)
                    .foregroundStyle(.secondary)
                    .textSelection(.enabled)
                    .accessibilityIdentifier("assistant.corpus.status")
                if !discussion.corpusDiagnostics.isEmpty {
                    DisclosureGroup("Diagnostics (\(discussion.corpusDiagnostics.count))") {
                        ForEach(Array(discussion.corpusDiagnostics.enumerated()), id: \.offset) { _, item in
                            Text(item)
                                .workbenchFont(.caption2)
                                .foregroundStyle(.secondary)
                                .textSelection(.enabled)
                        }
                    }
                    .workbenchFont(.caption2)
                    .accessibilityIdentifier("assistant.corpus.diagnostics")
                }
            }
        }
        .padding(16)
        .frame(width: 390)
    }

    private func primaryAttachmentLabel(_ discussion: AssistantDiscussionState) -> String {
        guard let attachment = discussion.activeConversation?.attachments.first(where: \.primary) else {
            return "Attached to this project"
        }
        return "Attached to \(attachment.label)"
    }

    private func selectedModelLabel(_ discussion: AssistantDiscussionState) -> String {
        let id = discussion.activeConversation?.profile.model ?? ""
        return discussion.models.first(where: { $0.id == id })?.label ?? (id.isEmpty ? "Model" : id)
    }

    private func selectedEfforts(_ discussion: AssistantDiscussionState) -> [String] {
        let id = discussion.activeConversation?.profile.model ?? ""
        return discussion.models.first(where: { $0.id == id })?.supportedEfforts
            ?? ["low", "medium", "high"]
    }

    private func accountAndUsage(_ discussion: AssistantDiscussionState) -> String {
        let plan = discussion.account.plan?.capitalized ?? "ChatGPT"
        if let used = discussion.usage.primaryPercentUsed {
            return "\(plan) · \(Int(max(0, 100 - used)))% remaining"
        }
        return plan
    }
}
